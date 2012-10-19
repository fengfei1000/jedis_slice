package fengfei.redis.slice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import fengfei.redis.utils.Hashed;

/**
 * key -> hash % size->slice
 * 
 * 
 */
public class HashEqualizer implements Equalizer {

	protected Hashed hashed = Hashed.MD5;
	protected Map<Long, RedisSlice> sliceMap;

	@Override
	public RedisSlice get(String key, int size) {
		long sk = Math.abs(hashed.hash32(key) % size);
		return sliceMap.get(sk);
	}

	@Override
	public void mapSlice(Map<Long, RedisSlice> redisSliceMap) {
		sliceMap = new ConcurrentHashMap<>(redisSliceMap);
	}
}
