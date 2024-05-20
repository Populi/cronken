import asyncio
import hashlib
import json
import logging
import struct
import time
import warnings
from asyncio import Task
from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from socket import AF_INET, SOCK_DGRAM, gethostname, socket
from typing import Deque, Dict, List, Optional, Union
from uuid import uuid4

from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from coredis import Redis, RedisCluster
from coredis.commands.pubsub import ClusterPubSub, PubSub
from coredis.commands.script import Script
from coredis.exceptions import LockAcquisitionError, LockReleaseError, LockError
from coredis.recipes.locks import LuaLock
from coredis.typing import Node
from pydantic import ValidationError

from .json_models import JobDef

# Suppress pytz deprecation warning that should be fixed in the next version of APScheduler
warnings.filterwarnings("ignore", message="The localize method is no longer necessary, "
                                          "as this time zone supports the fold attribute")


SOURCE_DIR = Path(__file__).parent.absolute()


class LockReleaseTimeoutError(LockReleaseError):
    pass


class Lock(LuaLock):
    def __init__(self, *args, release_timeout: int = 5, **kwargs):
        self.release_timeout = release_timeout
        super().__init__(*args, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await asyncio.wait_for(super().__aexit__(exc_type, exc_val, exc_tb), timeout=self.release_timeout)
        except TimeoutError:
            raise LockReleaseTimeoutError()


# From https://stackoverflow.com/a/28950776
# Finds the IP address of the local interface that routes to a remote address
def get_local_ip(remote_routable_addr: str):
    s = socket(AF_INET, SOCK_DGRAM)
    s.settimeout(0)
    try:
        # We're not actually trying to contact the IP, just see which interface routes it
        s.connect((remote_routable_addr, 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


# Dummy trigger for @never
class NeverTrigger(BaseTrigger):
    def __init__(self):
        pass

    def get_next_fire_time(self, previous_fire_time, now):
        return None


class Cronken:
    scheduler: Optional[AsyncIOScheduler] = None
    jobs: Dict[str, Job] = {}
    known_runs: Dict[str, asyncio.subprocess.Process] = {}
    rclient: Union[Redis, RedisCluster, None] = None
    pubsub: Union[PubSub, ClusterPubSub, None] = None
    scripts: Dict[str, Script] = {}
    tasks: Dict[str, Task] = {}
    state: Dict[str, int] = {}
    lifetime: Optional[asyncio.Future] = None

    VALID_COMMANDS = {
        'pause',
        'resume',
        'terminate_run',
        'kill_run',
        'drain_nodes',
        'add',
        'remove',
        'reload',
        'trigger',
        'validate'
    }

    # Cribbed from https://man7.org/linux/man-pages/man5/crontab.5.html#EXTENSIONS
    SCHEDULE_SHORTHAND = {
        "@yearly": CronTrigger.from_crontab("0 0 1 1 *"),
        "@annually": CronTrigger.from_crontab("0 0 1 1 *"),
        "@monthly": CronTrigger.from_crontab("0 0 1 * *"),
        "@weekly": CronTrigger.from_crontab("0 0 * * 0"),
        "@daily": CronTrigger.from_crontab("0 0 * * *"),
        "@hourly": CronTrigger.from_crontab("0 * * * *"),
        "@minutely": CronTrigger.from_crontab("* * * * *"),
        "@never": NeverTrigger()
    }

    def __init__(self,
                 redis_info: Union[List[Dict], Dict],
                 namespace: str = "{cronken}",
                 log_level: str = "DEBUG",
                 heartbeat_cadence: int = 30,
                 output_cadence: int = 5,
                 max_finalized_output_lines: int = 10,
                 perjob_results_limit: int = 1000,
                 general_results_limit: int = 10000,
                 output_buffer_size: int = 1024,
                 pubsub_timeout: int = 30,
                 job_shell: str = "/bin/bash",
                 graceful_cleanup: bool = False,
                 **kwargs):

        # If we're just passed a single {"host": "foo", "port": 1234} dict, wrap it in an array to standardize it
        if type(redis_info) is dict:
            redis_info = [redis_info]
        self.redis_info = redis_info
        self.namespace = namespace
        self.heartbeat_cadence = heartbeat_cadence
        self.output_cadence = output_cadence
        self.max_finalized_output_lines = max_finalized_output_lines
        self.perjob_results_limit = perjob_results_limit
        self.general_results_limit = general_results_limit
        self.output_buffer_size = output_buffer_size
        self.pubsub_timeout = pubsub_timeout
        self.job_shell = job_shell
        self.graceful_cleanup = graceful_cleanup

        self.host = gethostname()
        self.my_ip = get_local_ip(self.redis_info[0]["host"])
        self.draining = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level.upper())
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(log_level.upper())
        formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stdout_handler.setFormatter(formatter)
        self.logger.addHandler(stdout_handler)

    async def start(self) -> asyncio.Future:
        self.logger.debug("Starting main loop")
        self.logger.debug("Initializing redis connection")
        nodes = [Node(**x) for x in self.redis_info]
        # Assume we're connecting to a cluster if there's more than one node
        if len(nodes) > 1:
            self.rclient = RedisCluster(startup_nodes=nodes)
        else:
            self.rclient = Redis(host=nodes[0]["host"], port=nodes[0]["port"])
        self.pubsub = self.rclient.pubsub(ignore_subscribe_messages=True)

        self.logger.debug("Loading lua scripts into redis")
        for lua_file in SOURCE_DIR.glob('*.lua'):
            self.scripts[lua_file.stem] = self.rclient.register_script(lua_file.read_text())
        self.logger.debug(f"Loaded scripts: {list(self.scripts.keys())}")

        self.logger.debug("Starting cronken")
        self.scheduler = AsyncIOScheduler()
        self.scheduler.start()

        self.logger.debug("Loading jobs")
        await self.reload_jobs()

        self.logger.debug("Starting events task")
        self.tasks["events"] = asyncio.create_task(self.listen_for_events())

        self.logger.debug("Starting instance heartbeat task")
        self.tasks["heartbeat"] = asyncio.create_task(self.run_instance_heartbeat())

        self.lifetime = asyncio.Future()

        self.logger.debug("Initialization complete")
        return self.lifetime

    async def cleanup(self):
        for name, task in self.tasks.items():
            try:
                task.cancel()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.exception(f"Task {name} hit exception during cleanup: {e!r}")
        self.tasks = {}

        if self.pubsub:
            self.pubsub.close()

        # Wait for in-flight jobs to finish if graceful_cleanup is set
        if self.graceful_cleanup:
            for job in self.jobs.values():
                job.pause()
            while len(self.known_runs) > 0:
                await asyncio.sleep(0.5)

        if self.scheduler:
            try:
                self.scheduler.shutdown()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.exception(f"Scheduler hit exception during cleanup: {e!r}")

        self.logger.debug("Cleanup finished")
        self.lifetime.set_result(True)

    async def num_jobs(self):
        return await self.rclient.hlen(f"{self.namespace}:jobs")

    async def reload_jobs(self):
        new_jobs = await self.get_jobs()

        # First clear out the old jobs, if any
        for job_name in self.jobs:
            self.jobs[job_name].remove()
        self.jobs = {}

        # Then load in the new ones
        for job_name, job_def in new_jobs.items():
            self.logger.info(f"loading job {job_name} with def {job_def}")
            self.jobs[job_name] = self.job_add(job_name, job_def)

        # Add in the reaper
        # Trigger once every heartbeat
        self.logger.info("Loading reaper")
        trigger = IntervalTrigger(seconds=self.heartbeat_cadence)
        self.jobs["__reaper__"] = self.scheduler.add_job(func=self.run_reaper, trigger=trigger)

    async def lock_extender(self, acquired_lock: Lock, ttl: float, run_id: str = "Unknown"):
        interval = float(ttl) / 2
        while True:
            await asyncio.sleep(interval)
            try:
                await acquired_lock.extend(interval)
            except LockError as e:
                self.logger.warning(f"[{run_id}] Lock extender failed with exception {e!r}")
                break

    async def run_heartbeat(self, run_id: str):
        run_heartbeat: Script = self.scripts["run_heartbeat"]
        heartbeat_key = f"{self.namespace}:rundata:active"
        while True:
            await asyncio.sleep(self.heartbeat_cadence)
            await run_heartbeat([heartbeat_key], [run_id])

    async def store_output(self, output_key: str, output_buffer: Deque[bytes], final=False):
        # Prepend a ... to the output buffer if we're at max length to indicate dropped bytes
        output_list = [b'...\n'] if len(output_buffer) == output_buffer.maxlen else []
        output_list.extend(b''.join(output_buffer).splitlines(keepends=True))
        output_buffer.clear()
        # If we're not the final output and we don't end with a newline, put the last line back in the buffer
        if output_list and not final and not output_list[-1].endswith(b'\n'):
            last_fragment = output_list.pop()
            output_buffer.extend(struct.unpack(f'{len(last_fragment)}c', last_fragment))

        if output_list:
            await self.rclient.rpush(output_key, output_list)

    async def run_output(self, output_key: str, output_buffer: Deque[bytes]):
        while True:
            await asyncio.sleep(self.output_cadence)
            await self.store_output(output_key, output_buffer)

    @property
    def status(self):
        if not self.draining:
            return "running"
        if len([k for k, v in self.known_runs.items() if v.returncode is None]) > 0:
            return "draining"
        return "drained"

    async def run_instance_heartbeat(self):
        instance_heartbeat: Script = self.scripts["instance_heartbeat"]
        instance_heartbeat_key = f"{self.namespace}:instances"

        while True:
            await asyncio.sleep(self.heartbeat_cadence)
            await instance_heartbeat([instance_heartbeat_key], [self.host, self.my_ip])

    async def run_reaper(self):
        cadence: int = self.heartbeat_cadence
        run_get_expired: Script = self.scripts["run_get_expired"]
        run_finalize: Script = self.scripts["run_finalize"]
        instance_reap: Script = self.scripts["instance_reap"]

        instance_heartbeat_key = f"{self.namespace}:instances"
        rundata_key = f"{self.namespace}:rundata"
        heartbeat_key = f"{self.namespace}:rundata:active"
        results_fail_key = f"{self.namespace}:results:fail"

        lock_name = f"{self.namespace}:locks:__reaper__"

        try:
            async with Lock(self.rclient, lock_name, blocking_timeout=0.1, timeout=10) as acquired_lock:
                self.logger.debug("Running reaper!")
                extend_task = asyncio.create_task(self.lock_extender(acquired_lock, 10, run_id="reaper"))
                try:
                    # Reap instances with stale heartbeats
                    # Use 1.5x the heartbeat cadence so we avoid edge cases where the heartbeat is slightly late
                    await instance_reap([instance_heartbeat_key], [cadence+(cadence//2)])
                    # Get the list of expired runs
                    expired_runs_str = await run_get_expired([rundata_key, heartbeat_key], [cadence+(cadence//2)])
                    expired_runs = json.loads(expired_runs_str)
                    self.logger.debug(f"Expiring runs: {expired_runs}")
                    for run_id, job_name in expired_runs.items():
                        # If the run_id doesn't exist in rundata, just drop it from the active set
                        if not job_name:
                            await self.rclient.hdel(heartbeat_key, [run_id])
                            continue

                        output_key = f"{self.namespace}:rundata:output:{run_id}"
                        perjob_fail_key = f"{self.namespace}:results:{job_name}:fail"
                        await run_finalize(
                            keys=[rundata_key, heartbeat_key, output_key, results_fail_key, perjob_fail_key],
                            args=[
                                run_id,
                                "no_retcode",
                                "frozen",
                                self.max_finalized_output_lines,
                                self.general_results_limit,
                                self.perjob_results_limit
                            ]
                        )
                finally:
                    extend_task.cancel()
                    try:
                        await extend_task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        self.logger.exception(f"Reaper extend task hit exception: {e!r}")
        except LockAcquisitionError:
            # Someone else is running the reaper, so do nothing
            return
        except LockError:
            self.logger.warning(f"Reaper lock hit error {e!r}")

    async def job_run(self, job_name: str, cmd: str, lock: Union[bool, str], ttl: int):
        run_init: Script = self.scripts["run_init"]
        run_finalize: Script = self.scripts["run_finalize"]
        run_id = uuid4().hex
        output_buffer = deque(maxlen=self.output_buffer_size)
        rundata_key = f"{self.namespace}:rundata"
        heartbeat_key = f"{self.namespace}:rundata:active"
        output_key = f"{self.namespace}:rundata:output:{run_id}"
        # Initialize the lock to a default value based on the job if it's set to True
        if lock is True:
            lock = job_name
        if lock:
            lock_name = f"{self.namespace}:locks:{hashlib.sha1(lock.encode('utf-8')).hexdigest()}"
            try:
                async with Lock(self.rclient, lock_name, blocking_timeout=0.1, timeout=ttl) as acquired_lock:
                    # Add the run to rundata
                    await run_init([rundata_key, heartbeat_key], [run_id, job_name, self.host])
                    # Start background tasks
                    tasks = [
                        asyncio.create_task(self.lock_extender(acquired_lock, ttl, run_id=run_id)),
                        asyncio.create_task(self.run_heartbeat(run_id)),
                        asyncio.create_task(self.run_output(output_key, output_buffer)),
                    ]
                    # Actually run the job
                    self.logger.info(f"[{run_id}] Lock {lock} acquired, running job {job_name}")
                    start_time = time.monotonic()
                    proc, ret_code = None, "no_retcode"
                    try:
                        # Start the process and combine stdout/stderr into a single stream
                        proc = await asyncio.create_subprocess_shell(cmd,
                                                                     executable=self.job_shell,
                                                                     stdout=asyncio.subprocess.PIPE,
                                                                     stderr=asyncio.subprocess.STDOUT)
                        self.known_runs[run_id] = proc
                        while new_output := await proc.stdout.read(self.output_buffer_size):
                            # Annoyingly, struct.unpack is the fastest way to put bytes on a deque without external
                            # libraries by orders of magnitude, see https://stackoverflow.com/a/57543519
                            output_buffer.extend(struct.unpack(f'{len(new_output)}c', new_output))
                    finally:
                        # Stop all the background tasks
                        [task.cancel() for task in tasks]
                        for task in tasks:
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                            except Exception as e:
                                self.logger.exception(f"[{run_id}] hit exception while cancelling: {e!r}")

                        # Extract any remaining output and make sure there's a return code present
                        if proc is not None:
                            final_output, _ = await proc.communicate()
                            output_buffer.extend(struct.unpack(f'{len(final_output)}c', final_output))
                            ret_code = proc.returncode
                        # Store any remaining output
                        if output_buffer:
                            await self.store_output(output_key, output_buffer, final=True)
                        # Drop the run from known_runs if it exists
                        self.known_runs.pop(run_id, None)
            except LockAcquisitionError:
                self.logger.debug(f"[{run_id}] Lock {lock} already acquired, skipping job {job_name}")
                return
            except LockReleaseError as e:
                self.logger.warning(f"[{run_id}] failed to release lock: {str(e)}")
            except LockError as e:
                self.logger.warning(f"[{run_id}] Unknown LockError: {e!r}")
            except Exception as e:
                self.logger.warning(f"[{run_id}] Unknown exception: {e!r}")
        else:
            # Add the run to rundata
            await run_init([rundata_key, heartbeat_key], [run_id, job_name, self.host])
            # Start the background tasks
            tasks = [
                asyncio.create_task(self.run_heartbeat(run_id)),
                asyncio.create_task(self.run_output(output_key, output_buffer)),
            ]
            # Actually run the job
            self.logger.debug(f"[{run_id}] Running lockless job {job_name}")
            proc, ret_code = None, "no_retcode"
            start_time = time.monotonic()
            try:
                # Start the process and combine stdout/stderr into a single stream
                proc = await asyncio.create_subprocess_shell(cmd,
                                                             executable=self.job_shell,
                                                             stdout=asyncio.subprocess.PIPE,
                                                             stderr=asyncio.subprocess.STDOUT)
                self.known_runs[run_id] = proc
                while new_output := await proc.stdout.read(self.output_buffer_size):
                    # Annoyingly, struct.unpack is the fastest way to put bytes on a deque without external
                    # libraries by orders of magnitude, see https://stackoverflow.com/a/57543519
                    output_buffer.extend(struct.unpack(f'{len(new_output)}c', new_output))

            finally:
                # Stop all the background tasks
                [task.cancel() for task in tasks]
                for task in tasks:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        self.logger.exception(f"[{run_id}] hit exception while cancelling: {e!r}")

                # Extract any remaining output and make sure there's a return code present
                if proc is not None:
                    final_output, _ = await proc.communicate()
                    output_buffer.extend(struct.unpack(f'{len(final_output)}c', final_output))
                    ret_code = proc.returncode
                # Store any remaining output
                if output_buffer:
                    await self.store_output(output_key, output_buffer, final=True)
                # Drop the run from known_runs if it exists
                self.known_runs.pop(run_id, None)

        end_time = time.monotonic()
        duration = end_time - start_time
        self.state[f"job_{job_name}"] = ret_code

        # Tell redis that the job is done
        status = "success" if ret_code == 0 else "fail"
        general_result_key = f"{self.namespace}:results:{status}"
        perjob_result_key = f"{self.namespace}:results:{job_name}:{status}"
        output = await run_finalize(
            keys=[rundata_key, heartbeat_key, output_key, general_result_key, perjob_result_key],
            args=[run_id, ret_code, status, self.max_finalized_output_lines, self.general_results_limit, self.perjob_results_limit]
        )

        self.logger.info(f"[{run_id}] job_name: {job_name} cmd:{cmd} lock:{lock} ttl:{ttl} duration: {duration} "
                         f"host: {self.host} retcode: {ret_code} output: {output}")

    def job_add(self, job_name, job_def):
        if "cronstring" in job_def["cron_args"]:
            if job_def["cron_args"]["cronstring"] in self.SCHEDULE_SHORTHAND:
                trigger = self.SCHEDULE_SHORTHAND[job_def["cron_args"]["cronstring"]]
            else:
                trigger = CronTrigger.from_crontab(job_def["cron_args"]["cronstring"])
        else:
            # Assume that the args are keyword args and feed them directly to CronTrigger as such
            trigger = CronTrigger(**job_def["cron_args"])
        paused = job_def.get("job_state", {}).get("paused", False)
        add_job_kwargs = {
            "name": job_name,
            "func": self.job_run,
            "args": [job_name],
            "kwargs": job_def["job_args"],
            "trigger": trigger
        }
        # If the job is paused or we're triggering on @never, start the job in a paused state
        if isinstance(trigger, NeverTrigger) or paused:
            add_job_kwargs["next_run_time"] = None
        job = self.scheduler.add_job(**add_job_kwargs)
        return job

    async def get_jobs(self) -> Dict:
        # If the key doesn't exist, this will return None, so replace it with a blank dict
        raw_jobs = (await self.rclient.hgetall(f"{self.namespace}:jobs")) or {}
        valid_jobs = {}
        for k, v in raw_jobs.items():
            try:
                valid_jobs[k.decode('utf-8')] = JobDef.model_validate_json(v).model_dump(exclude_none=True)
            except (UnicodeDecodeError, ValidationError) as e:
                # Skip any jobs that fail to validate for whatever reason
                self.logger.error(f"get_jobs: Failed to load {k} job definition (raw {v}): {e!r}")
        return valid_jobs

    async def set_jobs(self, jobs_dict: Dict):
        # Set jobs replaces what's in the key with what's in jobs_dict by deleting the old one first
        # We do this in a transaction so we never delete without also replacing
        async with await self.rclient.pipeline(transaction=True) as pipe:
            await pipe.delete(keys=[f"{self.namespace}:jobs"])
            await pipe.hset(key=f"{self.namespace}:jobs", field_values={k: json.dumps(v) for k, v in jobs_dict.items()})
            await pipe.execute()

    async def update_jobs(self, jobs_dict: Dict):
        # Update jobs only overwrites the keys specified in jobs_dict, leaving old/absent keys alone
        await self.rclient.hset(
            key=f"{self.namespace}:jobs",
            field_values={k: json.dumps(v) for k, v in jobs_dict.items()}
        )

    async def validate_jobs(self, job_names:Union[List[str], str, None]) -> int:
        # We only want a single validation process to be happening at once
        lock_name = f"{self.namespace}:locks:__validation__"
        try:
            async with Lock(self.rclient, lock_name, blocking_timeout=0.1, timeout=60) as acquired_lock:
                if job_names:
                    if type(job_names) is str:
                        job_names = [job_names]
                    raw_job_defs = await self.rclient.hmget(f"{self.namespace}:jobs", job_names)
                    raw_jobs = {job_names[i]: raw_job_defs[i] for i in range(len(raw_job_defs)) if raw_job_defs[i]}
                else:
                    # If we're not passed a list of job names, validate them all
                    raw_jobs = (await self.rclient.hgetall(f"{self.namespace}:jobs")) or {}

                updated_jobs = {}
                for k, v in raw_jobs.items():
                    try:
                        # Use a defaultdict for the better ergonomics of being able to set
                        # ["job_state"]["validation_errors"] without having to worry whether ["job_state"] exists
                        job_def = defaultdict(dict, json.loads(v))
                    except json.JSONDecodeError as e:
                        # Nothing we can do if the JSON fails to decode
                        self.logger.error(f"validate_jobs: Failed to decode the json for job {k} (raw {v}): {e}")
                        continue
                    try:
                        # Skip any jobs that validate
                        k.decode('utf-8')
                        JobDef.model_validate(job_def)
                        continue
                    except (UnicodeDecodeError, ValidationError) as e:
                        # It's assumed that updated jobs are written without the validation_errors key,
                        # so only update definitions that don't already have it
                        if "validation_errors" not in job_def["job_state"]:
                            self.logger.warning(f"Setting errors for job {k}: {e}")
                            job_def["job_state"]["validation_errors"] = repr(e)
                            updated_jobs[k] = json.dumps(job_def)
                if updated_jobs:
                    await self.rclient.hset(key=f"{self.namespace}:jobs", field_values=updated_jobs)
                # Returns number of jobs that were updated with validation errors
                return len(updated_jobs)
        except LockError:
            # Someone else is already running a validation, so do nothing
            return

    async def send_event(self, raw_event: str):
        # Validate event to make sure it's at least semi-kosher
        parsed = json.loads(raw_event)
        action = parsed.get('action', None)
        if action not in self.VALID_COMMANDS:
            raise Exception(f"Invalid action {str(action)}")
        # Actually send the event
        await self.rclient.publish(channel=f"{self.namespace}:__events__", message=raw_event.encode('utf-8'))

    async def handle_event(self, event: Dict):
        if not all(x in event for x in ("action", "args")):
            self.logger.warning(f"EVENT: Skipping malformed event {event}")
            return

        if event["action"] == "pause":
            job_name = event["args"]
            # Special case "all" to loop through all jobs
            if job_name == "all":
                for job_name in self.jobs:
                    self.jobs[job_name].pause()
                    self.logger.info(f"EVENT: Paused job {job_name}")
                self.logger.info("EVENT: Paused all jobs")
                return

            if job_name not in self.jobs:
                self.logger.warning(f"EVENT: Unknown job {job_name}, cannot pause")
                return
            self.jobs[job_name].pause()
            self.logger.info(f"EVENT: Paused job {job_name}")
        elif event["action"] == "resume":
            job_name = event["args"]
            # Special case "all" to loop through all jobs
            if job_name == "all":
                for job_name in self.jobs:
                    self.jobs[job_name].resume()
                    self.logger.info(f"EVENT: Resumed job {job_name}")
                self.logger.info("EVENT: Resumed all jobs")
                return

            if job_name not in self.jobs:
                self.logger.warning(f"EVENT: Unknown job {job_name}, cannot resume")
                return
            self.jobs[job_name].resume()
            self.logger.info(f"EVENT: Resumed job {job_name}")
        elif event["action"] == "terminate_run":
            run_id = event["args"]
            if run_id in self.known_runs:
                self.known_runs[run_id].terminate()
                self.logger.info(f"EVENT: Terminated run {run_id}")
            else:
                self.logger.warning(f"EVENT: Unknown run {run_id}, cannot terminate")
        elif event["action"] == "kill_run":
            run_id = event["args"]
            if run_id in self.known_runs:
                self.known_runs[run_id].kill()
                self.logger.info(f"EVENT: Killed run {run_id}")
            else:
                self.logger.warning(f"EVENT: Unknown run {run_id}, cannot kill")
        elif event["action"] == "drain_nodes":
            nodes = event["args"]
            if self.host in nodes:
                self.logger.info(f"EVENT: Draining node {self.host}")
                self.draining = True
                for job_name in self.jobs:
                    self.jobs[job_name].pause()
            else:
                self.logger.info(f"EVENT: Skipping node drain because {self.host} is not on the list")
        elif event["action"] == "add":
            if not all(x in event['args'] for x in ('job_name', 'job_def')):
                self.logger.warning(f"EVENT: Skipping malformed add event {event}")
                return
            job_name, job_def = event['args']['job_name'], event['args']['job_def']
            if job_name in self.jobs:
                self.logger.warning(f"EVENT: Job {job_name} already exists, skipping add operation {event}")
                return
            self.jobs[job_name] = self.job_add(job_name, job_def)
            self.logger.info(f"EVENT: Added job {job_name} with definition {job_def}")
        elif event["action"] == "remove":
            job_name = event["args"]
            if job_name not in self.jobs:
                self.logger.warning(f"EVENT: Job {job_name} does not exist, skipping remove operation {event}")
                return
            self.jobs[job_name].remove()
            self.jobs.pop(job_name)
            self.logger.info(f"EVENT: removed job {job_name}")
        elif event["action"] == "reload":
            await self.reload_jobs()
            self.logger.info(f"EVENT: reloaded jobs from redis")
        elif event["action"] == "validate":
            job_names = event["args"]
            num_errors = await self.validate_jobs(job_names)
            if job_names:
                self.logger.info(f"EVENT: validated job(s) {job_names}.  Found {num_errors} errors")
            else:
                self.logger.info(f"EVENT: validated jobs.  Found {num_errors} errors")
        elif event["action"] == "trigger":
            job_name = event["args"]
            if job_name not in self.jobs:
                self.logger.warning(f"EVENT: Job {job_name} does not exist, skipping trigger operation {event}")
                return
            known_job = self.jobs[job_name]
            # Schedule an ephemeral copy of the job to run once and then get removed from the scheduler
            self.scheduler.add_job(
                name=f"{job_name}_triggered_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                func=known_job.func,
                args=known_job.args,
                kwargs=known_job.kwargs,
                trigger=NeverTrigger(),
                next_run_time=datetime.now()
            )
            self.logger.info(f"EVENT: triggered job {job_name}")
        else:
            self.logger.warning(f"EVENT: Unknown action type {event['action']} for event {event}")

    async def listen_for_events(self):
        timeout: int = self.pubsub_timeout
        await self.pubsub.subscribe(f"{self.namespace}:__events__")
        while True:
            raw_message = await self.pubsub.get_message(timeout=timeout)
            if raw_message:
                if raw_message["type"] != "message":
                    continue
                try:
                    event = json.loads(raw_message["data"].decode('utf-8'))
                except json.JSONDecodeError:
                    self.logger.error(f"Couldn't parse message {raw_message['data']}")
                    continue
                try:
                    await self.handle_event(event)
                except Exception as e:
                    self.logger.exception(f"Encountered exception {e!r} while handling event {event}")
                    continue
            await asyncio.sleep(0.001)
