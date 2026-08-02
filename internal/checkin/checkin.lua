if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end

redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return 1
