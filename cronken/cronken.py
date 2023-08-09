import struct
import json
import time
from asyncio import Task
from collections import deque
from socket import gethostname, socket, AF_INET, SOCK_DGRAM

import hashlib
import asyncio
import logging
from datetime import datetime
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from coredis import Redis, RedisCluster
from coredis.typing import Node
from coredis.recipes.locks import LuaLock as Lock
from coredis.commands.script import Script
from coredis.commands.pubsub import PubSub, ClusterPubSub
from coredis.exceptions import LockError
from typing import Dict, Optional, Union, Deque, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.base import BaseTrigger
from apscheduler.job import Job

# Suppress pytz deprecation warning that should be fixed in the next version of APScheduler
import warnings
warnings.filterwarnings("ignore", message="The localize method is no longer necessary, "
                                          "as this time zone supports the fold attribute")


SOURCE_DIR = Path(__file__).parent.absolute()


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

    VALID_COMMANDS = {
        'pause',
        'resume',
        'terminate_run',
        'kill_run',
        'add',
        'remove',
        'reload',
        'trigger'
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
                 job_shell: str = "/bin/bash"):

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

        self.host = gethostname()
        self.my_ip = get_local_ip(self.redis_info[0]["host"])
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level.upper())

    async def main_loop(self):
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

        self.logger.debug("Initialization complete")

    async def cleanup(self):
        for task in self.tasks.values():
            task.cancel()
            with suppress(asyncio.exceptions.CancelledError):
                await task
        self.tasks = {}

        with suppress(asyncio.exceptions.CancelledError):
            self.scheduler.shutdown()

        self.pubsub.close()
        self.rclient.close()

        self.logger.debug("Cleanup finished")

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

    async def lock_extender(self, acquired_lock: Lock, ttl: float):
        interval = float(ttl) / 2
        while True:
            await asyncio.sleep(interval)
            try:
                await acquired_lock.extend(interval)
            except LockError as e:
                self.logger.warning(f"Lock extender failed with exception {e!r}")
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
                extend_task = asyncio.create_task(self.lock_extender(acquired_lock, 10))
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
                    with suppress(asyncio.CancelledError):
                        await extend_task
        except LockError:
            # Someone else is running the reaper, so do nothing
            return

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
                        asyncio.create_task(self.lock_extender(acquired_lock, ttl)),
                        asyncio.create_task(self.run_heartbeat(run_id)),
                        asyncio.create_task(self.run_output(output_key, output_buffer)),
                    ]
                    # Actually run the job
                    self.logger.info(f"Lock {lock} acquired, running job {job_name}")
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
                            with suppress(asyncio.CancelledError):
                                await task
                        # Extract any remaining output and make sure there's a return code present
                        if proc:
                            final_output, _ = await proc.communicate()
                            output_buffer.extend(struct.unpack(f'{len(final_output)}c', final_output))
                            ret_code = proc.returncode
                        # Store any remaining output
                        await self.store_output(output_key, output_buffer, final=True)
                        # Drop the run from known_runs if it exists
                        self.known_runs.pop(run_id, None)

            except LockError as e:
                self.logger.warning(f"Couldn't acquire lock for {job_name} ({cmd}) due to {e!r}")
                return
        else:
            # Add the run to rundata
            await run_init([rundata_key, heartbeat_key], [run_id, job_name, self.host])
            # Start the background tasks
            tasks = [
                asyncio.create_task(self.run_heartbeat(run_id)),
                asyncio.create_task(self.run_output(output_key, output_buffer)),
            ]
            # Actually run the job
            self.logger.debug(f"Running lockless job {job_name}")
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
                    with suppress(asyncio.CancelledError):
                        await task
                # Extract any remaining output and make sure there's a return code present
                if proc:
                    final_output, _ = await proc.communicate()
                    output_buffer.extend(struct.unpack(f'{len(final_output)}c', final_output))
                    ret_code = proc.returncode
                # Store any remaining output
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

        self.logger.info(f"job_name: {job_name} cmd:{cmd} lock:{lock} ttl:{ttl} duration: {duration} "
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
        jobs = {}
        for k, v in raw_jobs.items():
            try:
                jobs[k.decode('utf-8')] = json.loads(v.decode('utf-8'))
            # Skip any entries that don't decode
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                self.logger.error(f"Failed to load {k} job definition (raw {v}): {e!r}")
        r
        # Update jobs only overwrites the keys specified in jobs_dict, leaving old/absent keys alone
        await self.rclient.hset(
            key=f"{self.namespace}:jobs",
            field_values={k: json.dumps(v) for k, v in jobs_dict.items()}
        )

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
        elif event["actionT: Job {job_name} does not exist, skipping trigger operation {event}")
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

