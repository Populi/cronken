--run_finalize [rundata_hash, heartbeat_set, output_set, completed_general, completed_perjob] [run_id, retcode, final_status, num_output_lines, max_results]
local rundata_hash, heartbeat_set, output_set, completed_general, completed_perjob = unpack(KEYS)
local run_id, retcode, final_status, num_output_lines, max_general_results, max_perjob_results = unpack(ARGV)
retcode = tonumber(retcode)
num_output_lines = tonumber(num_output_lines)

-- Start by getting the current timestamp
local epoch, microseconds = unpack(redis.call('TIME'))
local current_time = string.format("%d.%06d", epoch, microseconds)

-- We're no longer an active job, so remove the heartbeat
redis.call('ZREM', heartbeat_set, run_id)

-- Get the existing run metadata
local metadata_str = redis.call('HGET', rundata_hash, run_id)

-- If the run metadata doesn't exist, we can't do anything more
if type(metadata_str) ~= "string" then
  return metadata_str
end

-- Now that we know we have string metadata, JSON decode it
local metadata = cjson.decode(metadata_str)

-- Pull the last n output lines from the output set
local output = table.concat(redis.call('LRANGE', output_set, -1 * num_output_lines, -1), "")

-- Now that we have valid metadata and output, update the metadata
metadata.status = final_status
metadata.retcode = retcode
metadata.output = output or ""
metadata.duration = tonumber(current_time) - tonumber(metadata.start_time)
redis.call('HSET', rundata_hash, run_id, cjson.encode(metadata))

-- Add the run_id to the appropriate completed_sets
redis.call('ZADD', completed_general, current_time, run_id)
redis.call('ZADD', completed_perjob, current_time, run_id)

-- Trim completed_general.  This requires less work than perjob, because we're not dropping from rundata.
local general_result_cutoff = -1 * (tonumber(max_general_results) + 1)
redis.call('ZREMRANGEBYRANK', completed_general, 0, general_result_cutoff)

-- Drop the oldest from the perjob completed set if we're over max_results
-- If we want to keep 3 results, we want delete everything in the range 0 to -4 (4 from the end of the list)
local perjob_result_cutoff = -1 * (tonumber(max_perjob_results) + 1)
-- Check to see if there's anything to drop
local to_drop = redis.call('ZRANGE', completed_perjob, 0, perjob_result_cutoff)
-- Do the drop if there is
if next(to_drop) ~= nil then
  redis.call('HDEL', rundata_hash, unpack(to_drop))
  redis.call('ZREMRANGEBYRANK', completed_perjob, 0, perjob_result_cutoff)
  -- We also need to remove this from the general results queue because we've deleted the metadata
  redis.call('ZREM', completed_general, unpack(to_drop))
end

return output or ""

