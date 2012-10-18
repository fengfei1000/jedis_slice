package fengfei.redis.slice;

import java.util.List;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;

public class RedisSlice {

	protected RedisSliceInfo master;
	protected ObjectPool<Jedis> masterPool;
	protected RedisSliceInfo[] slaves;
	protected ObjectPool<Jedis>[] slavePools;
	protected int slaveSize;
	protected Plotter plotter;
	protected GenericObjectPool.Config config;

	public RedisSlice(RedisSliceInfo master, RedisSliceInfo[] slaves,
			Plotter plotter, GenericObjectPool.Config config) {
		super();
		this.master = master;
		this.slaves = slaves;
		this.slaveSize = slaves == null ? 0 : slaves.length;
		this.plotter = plotter;
		this.config = config;
		init();
	}

	public RedisSlice(RedisSliceInfo master, List<RedisSliceInfo> slaves,
			Plotter plotter, GenericObjectPool.Config config) {
		super();
		this.master = master;
		this.slaves = slaves == null ? null : (slaves
				.toArray(new RedisSliceInfo[slaves.size()]));
		this.slaveSize = slaves == null ? 0 : this.slaves.length;
		this.plotter = plotter;
		this.config = config;
		init();
	}

	private void init() {
		this.masterPool = new GenericObjectPool<>(new PoolableRedisFactory(
				master.host, master.port, master.timeout), config);
		if (slaves != null && slaves.length > 0) {
			slavePools = new GenericObjectPool[slaves.length];
			for (int i = 0; i < slaves.length; i++) {
				RedisSliceInfo slave = slaves[i];
				slavePools[i] = new GenericObjectPool<>(
						new PoolableRedisFactory(slave.host, slave.port,
								slave.timeout), config);
			}
		}
	}

	public ObjectPool<Jedis> getMaster(String key) {
		return masterPool;
	}

	public ObjectPool<Jedis> getNextSlave(String key) {
		if (slaves == null || slaves.length == 0) {
			return masterPool;
		}
		return slavePools[plotter.get(key, slaveSize)];
	}

	public ObjectPool<Jedis> getMaster(byte[] key) {
		return masterPool;
	}

	public ObjectPool<Jedis> getAny(byte[] key) {
		int index = plotter.get(key, slaveSize + 1);
		return (slavePools == null || index == slavePools.length) ? masterPool
				: slavePools[index];
	}

	public ObjectPool<Jedis> getNextSlave(byte[] key) {
		if (slaves == null || slaves.length == 0) {
			return masterPool;
		}
		return slavePools[plotter.get(key, slaveSize)];
	}

	public void close() throws Exception {
		masterPool.close();
		if (slavePools == null) {
			return;
		}
		for (ObjectPool<Jedis> objectPool : slavePools) {
			objectPool.close();
		}
	}
}
