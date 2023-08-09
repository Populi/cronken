--run_init [rundata_hash, heartbeat_set] [run_id, job_name, host]
local rundata_hash, heartbeat_set = unpack(KEYS)
local run_id, job_name, host = unpack(ARGV)

-- Construct the definition from the parameters
local epoch, microseconds = unpack(redis.call('TIME'))
local timestamp = string.format("%d.%06d", epoch, microseconds)
local job_info = {
    job_name=job_name,
    start_time=timestamp,
    host=host,
    status="running",
    retcode=nil,
    output=nil,
    duration=nil
}

-- Init the job id in rundata with the constructed definition
redis.call('HSET', rundata_hash, run_id, cjson.encode(job_info))

-- Add the first heartbeat
redis.call('ZADD', heartbeat_set, timestamp, run_id)