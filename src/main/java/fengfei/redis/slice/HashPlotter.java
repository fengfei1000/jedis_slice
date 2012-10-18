package fengfei.redis.slice;

import fengfei.redis.utils.Hashed;

public class HashPlotter implements Plotter {
	protected Hashed hashed = Hashed.MD5;

	@Override
	public int get(String key, int size) {
		return Math.abs(hashed.hash32(key) % size);
	}

	@Override
	public int get(byte[] key, int size) {
		return Math.abs(hashed.hash32(key) % size);
	}

}
