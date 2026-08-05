if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end

local stream_type = redis.call("TYPE", KEYS[2]).ok
if stream_type ~= "none" and stream_type ~= "stream" then
    return redis.error_reply("check-in event key is not a stream")
end

redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
redis.call(
    "XADD",
    KEYS[2],
    "*",
    "tournament_id", ARGV[3],
    "player_id", ARGV[4],
    "timestamp", ARGV[5]
)
return 1
