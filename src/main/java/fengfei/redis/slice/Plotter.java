package fengfei.redis.slice;

public interface Plotter {

	int get(String key, int size);

	int get(byte[] key, int size);

}
