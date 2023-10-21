# Definitions
## Job
A job is defined as a cron entry -- it's a paired trigger schedule and command that can cause Cronken to 
instantiate a run, executing the command and recording its result.  Jobs are stored in `{cronken}:jobs` and 
their paused/resumed state is stored in `{cronken}:jobs:paused`.
## Run
A run is one particular execution of a job.  It's assigned a unique run ID on creation, and its metadata is 
stored in `{cronken}:rundata`.  The live output for each run is stored in `{cronken}:rundata:output:<job_id>`,
and a heartbeat for the active run is stored in `{cronken}:rundata:active`.
## Result
A result is a pointer to the finalized metadata for a run.  Results are stored in size-limited sorted sets split
out by job name and success/failure, in `{cronken}:results:<job_name>:success` and 
`{cronken}:results:<job_name>:fail`.

# Cronken specification
## Job list: `{cronken}:jobs`
This is a Redis hash that maps each job name to a JSON string containing its definition.  The job list is loaded 
when cronken boots and when the `reload` command is issued through the command/control interface. An example job 
definition that runs a command once every two seconds, but is paused until a `resume` command is issued:
```json
{
  "cron_args": {
    "second": "*/2"
  },
  "job_args": {
    "cmd": "echo every_two_seconds",
    "lock": true,
    "ttl": 10
  },
  "job_state": {
    "paused": true
  }
}
```

Another example definition that runs once every minute, using a [cron expression](https://crontab.cronhub.io/) to
define its schedule:
```json
{
  "cron_args": {
    "cronstring": "* * * * *"
  },
  "job_args": {
    "cmd": "echo minutely",
    "lock": true,
    "ttl": 10
  }
}
```
Each job has two required keys (`cron_args` and `job_args`) and one optional key (`job_state`).  

`cron_args` contains either a raw `cronstring` or individually-specified second/minute/hour/day/etc as specified 
[here](https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html).

`job_args` has three required fields:
* `cmd`, which is a string that contains the command the job should run
* `lock`, which can be `false` to eschew the use of locks, `true` to use a lock with the name equal to the job name,  
  or an arbitrary string to use a lock named the contents of the string.  If this is set to `true` or a string, 
  all servers running cronken will attempt to acquire the lock before running `cmd`, and only the one that
  successfully acquires it will actually run it.
* `ttl`, which is an integer specifying how long the lock should persist if cronken is killed mid-execution.
  While `cmd` is running, the lock is automatically renewed, but in the event the program crashes or is killed, 
  the lock will automatically be released in `ttl` seconds.

`job_state` contains any state-based flags the job has.  Currently, this is only used for persisting paused state 
across restarts -- if job_state->paused is set to true, the job will be paused on load.  If it's set to false or
doesn't exist, the job will be loaded like normal.

## Run metadata: `{cronken}:rundata`
This is a Redis hash mapping from run ID to a JSON blob of the job status.  It's initialized just before the job
starts, and is "finalized" (updated with the status, retcode, output, and duration) once the job finishes.

The JSON blob is initialized like this:
```json
{
  <run_id>: {
    "job_name": <job_name>,
    "start_time": <timestamp>,
    "host": <hostname>,
    "status": "running",
    "retcode": null,
    "output": null,
    "duration": null
  }
}
```

Once a run finalizes, it looks like this:
```json
{
  <run_id>: {
    "job_name": <job_name>,
    "start_time": <timestamp>,
    "host": <hostname>,
    "status": "success/fail/frozen",
    "retcode": 0,
    "output": "last 10 lines of output",
    "duration": <float_in_seconds>
  }
}
```


## Active run heartbeats: `{cronken}:rundata:active`
This is a Redis sorted set of run IDs, with the last heartbeat timestamp as the "score" (sorting mechanism).
Whenever a run is started, a background task to update the heartbeat timestamp every 30 seconds is also started. 
That way, if a cronken instance is killed and the job definition is still in active, it should be easy to detect 
stale jobs and clear them out.

To clear stale jobs out, a reaper process is run every 30 seconds which examines the heartbeats and marks any runs
with a heartbeat greater than 45 seconds ago as stale, then finalizes them with the status "frozen" and removes
them from active.

## Run outputs: `{cronken}:rundata:output:<run_id>`
This is a Redis sorted set storing the lines of output with the timestamp of the line acting as the "score"
(sorting mechanism).  When a run is finalized, the last 10 lines (configurable) of output are copied into the
run metadata and immortalized.  The run output keys have a TTL of 24 hours after the last line is written, so
they'll automatically disappear and not take up increasing amounts of room on the server.

## Job results: `{cronken}:results:<success/fail>` and `{cronken}:results:<job_name>:<success/fail`
Whenever a run finalizes, it stores its results in the `{cronken}:rundata` key.  Separately, it adds the run_id 
to the general success/fail queue at `{cronken}:results:<success/fail>` as well as the job-specific success or
fail queue at `{cronken}:results:<job_name>:<success/fail>`, depending on the return code.  These keys are sorted 
sets that store the last n successful/failed job IDs with the timestamps they occurred.  Whenever a new job
completes, if there are already n successful or failed jobs of that job_name, it drops the oldest one from the
sorted set and deletes its job ID from the `{cronken}:rundata` hash.

## Job locks: `{cronken}:locks:<sha1_of_lock_name>`
This isn't something things outside of cronken itself can really interact with, but for the sake of completeness 
the locks for each job are stored at `{cronken}:locks:<sha1_of_lock_name>` using
[this method](https://coredis.readthedocs.io/en/latest/recipes/locks.html).

Also, the special lock `{cronken}:locks:__reaper__` is used to lock the reaper process that expires stale runs
whose heartbeat is too old.

## Command and control: `{cronken}:__events__`
Finally, cronken has a centralized command and control mechanism, done via a Redis pub/sub channel at
`{cronken}:__events__` that all cronken instances subscribe to on boot.  Events sent to this channel are
JSON-encoded strings in the following format:
```json
{
  "action": "name_of_action",
  "args": <action_specific_args>
}
```
Where `<action_specific_args>` is defined in the action-specific sections below.

Each subscribed cronken instance will receive these events and dispatch them to the appropriate handler. 
A listing of possible events follows.

### Event: `pause`
Causes cronken to pause future execution of a job, effectively disabling it while not stopping any already-started
instances.  Format:
```json
{
  "action": "pause",
  "args": <job_name>
}
```
Where `<job_name>` is a string containing the name of the job you wish to pause.  If the string is "all", all
jobs will be paused.

### Event: `resume`
Causes cronken to resume a previously-paused job, or does nothing if the job wasn't paused.  Format:
```json
{
  "action": "resume",
  "args": <job_name>
}
```
Where `<job_name>` is a string containing the name of the job you wish to resume.  If the string is "all",
all jobs will be resumed.

### Event: `terminate_run`
Causes cronken to send a SIGTERM to an in-process run.  Format:
```json
{
  "action": "terminate_run",
  "args": <run_id>
}
```
Where `<run_id>` is a string containing the run ID you wish to terminate.

### Event: `kill_run`
Causes cronken to send a SIGKILL to an in-process run.  Format:
```json
{
  "action": "kill_run",
  "args": <run_id>
}
```
Where `<run_id>` is a string containing the run ID you wish to kill.

### Event: `add`
Causes cronken to add a new job to its internal list.  Format:
```json
{
  "action": "add",
  "args": {
    "job_name": <job_name>,
    "job_def": <job_def>
  }
}
```
Where `job_name` is a string containing the name of the job you wish to create and `job_def` is a
[job definition](#job-list-cronjobs) from earlier on this page.

### Event: `remove`
Causes cronken to remove a job from its internal list.  Format:
```json
{
  "action": "remove",
  "args": <job_name>
}
```
Where `job_name` is a string containing the name of the job you wish to remove.

### Event: `reload`
Causes cronken to tear down all internal jobs and reload them fresh from the definitions contained in 
`{cronken}:jobs`.  Format:
```json
{
  "action: "reload",
  "args": {}
}
```

### Event: `trigger`
Causes cronken to immediately attempt to run a particular job without affecting the cron schedule it's set to
run on. Locking provisions still apply, so if locking is enabled on the job only a single server will
succcessfully run it. Format:
```json
{
  "action": "trigger",
  "args": <job_name>
}
```
Where `<job_name>` is a string containing the name of the job you wish to trigger.

### Event: `validate`
Causes cronken to validate one or more jobs, attempting to decode them and compare them to its JSON
spec.  Any jobs that parse as valid JSON but don't validate will be updated, and
job_def["job_state"]["validation_errors"] will be set to the text of the error.  An empty array of
job names will result in all jobs being validated. Format:
```json
{
  "action": "validate",
  "args": [<job_name>, <job_name>, ...]
}
```
