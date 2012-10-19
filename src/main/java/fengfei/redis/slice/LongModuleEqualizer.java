package fengfei.redis.slice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * key-> long % size->slice
 * 
 * 
 */
public class LongModuleEqualizer implements Equalizer {

	Map<Long, RedisSlice> sliceMap;

	@Override
	public RedisSlice get(String key, int size) {
		long mod = Long.parseLong(key);
		long sk = Math.abs(mod % size);
		return sliceMap.get(sk);
	}

	@Override
	public void mapSlice(Map<Long, RedisSlice> redisSliceMap) {
		sliceMap = new ConcurrentHashMap<>(redisSliceMap);

	}
}
