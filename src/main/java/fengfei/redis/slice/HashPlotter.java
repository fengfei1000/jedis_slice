package fengfei.redis.slice;

import fengfei.redis.Plotter;
import fengfei.redis.utils.Hashed;

public class HashPlotter implements Plotter {
	protected Hashed hashed = Hashed.MURMUR_HASH;

	@Override
	public int get(byte[] key, int size) {
		return Math.abs(hashed.hash32(key) % size);
	}

}
