package fengfei.redis.slice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import fengfei.redis.Equalizer;
import fengfei.redis.Plotter;
import fengfei.redis.utils.Hashed;

/**
 * key -> hash % size->slice
 * 
 * 
 */
public class HashEqualizer extends AbstractEqualizer implements Equalizer {

	protected Hashed hashed = Hashed.MD5;
	protected Map<Long, RedisSlice> sliceMap = new ConcurrentHashMap<>();

	public HashEqualizer() {

	}

	public HashEqualizer(int timeout, Config config, Plotter plotter) {
		super(timeout, config, plotter);
	}

	@Override
	public RedisSlice get(String key) {
		int size=getSliceMap().size();
		long sk = Math.abs(hashed.hash32(key) % size);
		return sliceMap.get(sk);
	}

	@Override
	public void mapSlice(Map<Long, RedisSlice> redisSliceMap) {
		sliceMap = new ConcurrentHashMap<>(redisSliceMap);
	}

	@Override
	public Map<Long, RedisSlice> getSliceMap() {
		return sliceMap;
	}
}
