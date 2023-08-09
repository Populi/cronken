--instance_heartbeat [instance_heartbeat_set] [host, ip]
local instance_heartbeat_set = unpack(KEYS)
local host, ip = unpack(ARGV)

-- Get the current timestamp
local epoch, microseconds = unpack(redis.call('TIME'))
local timestamp = string.format("%d.%06d", epoch, microseconds)

-- Update the heartbeat with the timestamp
redis.call('ZADD', instance_heartbeat_set, timestamp, cjson.encode({host = host, ip = ip}))