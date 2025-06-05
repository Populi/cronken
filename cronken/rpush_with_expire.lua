--rpush_with_expire [rpush_key] [ttl, *values]
local rpush_key = unpack(KEYS)
-- lua arrays start at 1 rather than 0
local ttl = ARGV[1]
ttl = tonumber(ttl)

-- unpack(ARGV, 2) is the equivalent of Python's *ARGV[1:]
-- that is to say, it takes all arguments after the first and unpacks them to separate args
local retval = redis.call('RPUSH', rpush_key, unpack(ARGV, 2))
if ttl > 0 then
    redis.call('EXPIRE', rpush_key, ttl)
end

return retval
