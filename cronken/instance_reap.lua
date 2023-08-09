--instance_reap [instance_heartbeat_set] [expire_seconds]
local instance_heartbeat_set = unpack(KEYS)
local expire_seconds = unpack(ARGV)
expire_seconds = tonumber(expire_seconds)

-- Get the current timestamp
local epoch, microseconds = unpack(redis.call('TIME'))
local current_time = string.format("%d.%06d", epoch, microseconds)

-- Remove all instances with a heartbeat older than the threshold
local expired_time = current_time - expire_seconds
redis.call('ZREMRANGEBYSCORE', instance_heartbeat_set, "-inf", tostring(expired_time))