package fengfei.redis.slice;

import java.util.concurrent.atomic.AtomicInteger;

public class LoopPlotter implements Plotter {
	protected AtomicInteger next = new AtomicInteger();

	@Override
	public int get(String key, int size) {
		return Math.abs(next.getAndIncrement() % size);
	}

	@Override
	public int get(byte[] key, int size) {
		return Math.abs(next.getAndIncrement() % size);
	}

}
