--run_get_expired [rundata_hash, heartbeat_set] [expire_seconds]
local rundata_hash, heartbeat_set = unpack(KEYS)
local expire_seconds = unpack(ARGV)
expire_seconds = tonumber(expire_seconds)

-- Abstract this to a function so we can catch exceptions
local function get_job_name(possible_string)
  return cjson.decode(possible_string).job_name
end

-- Get current timestamp from Redis
local epoch, microseconds = unpack(redis.call('TIME'))
local current_time = tonumber(string.format("%d.%06d", epoch, microseconds))

-- Get all runs with a heartbeat older than the threshold
local expired_time = current_time - expire_seconds
local expired_runs = redis.call('ZRANGEBYSCORE', heartbeat_set, "-inf", tostring(expired_time))

-- Enrich the data with the job name
local output = {}
for _, run_id in ipairs(expired_runs) do
    -- Get the rundata, if it exists
    local rundata = redis.call('HGET', rundata_hash, run_id)
    -- Attempt to decode it as json and pull out the job name
    local status, job_name = pcall(get_job_name, rundata)
    -- If the decode worked, add the run_id and job name to the output.  Otherwise, add nil.
    if status then
        output[run_id] = job_name
    else
        output[run_id] = nil
    end
end

return cjson.encode(output)