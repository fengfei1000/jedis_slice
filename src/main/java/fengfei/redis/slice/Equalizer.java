package fengfei.redis.slice;

import java.util.Map;

public interface Equalizer {
	RedisSlice get(String key, int size);

	void mapSlice(Map<Long, RedisSlice> redisSliceMap);
}
