--run_heartbeat [heartbeat_set] [run_id]
local heartbeat_set = unpack(KEYS)
local run_id = unpack(ARGV)

-- Get the current timestamp
local epoch, microseconds = unpack(redis.call('TIME'))
local timestamp = string.format("%d.%06d", epoch, microseconds)

-- Update the heartbeat with the timestamp
redis.call('ZADD', heartbeat_set, timestamp, run_id)