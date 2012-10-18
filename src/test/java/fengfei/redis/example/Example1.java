package fengfei.redis.example;

import org.apache.commons.pool.impl.GenericObjectPool;

import fengfei.redis.RedisComand;
import fengfei.redis.slice.HashBalance;
import fengfei.redis.slice.PoolableSlicedRedis;

public class Example1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PoolableSlicedRedis redis = new PoolableSlicedRedis(
				"127.0.0.1:6379 127.0.0.1:6380", 60000,
				new HashBalance(), new GenericObjectPool.Config());
		RedisComand rc = redis.createRedisCommand();
		for (int i = 0; i < 10; i++) {
			rc.set("K" + i, "V" + i);
		}
		redis.close();

	}
}
