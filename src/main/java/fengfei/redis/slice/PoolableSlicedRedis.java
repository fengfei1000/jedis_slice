package fengfei.redis.slice;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import fengfei.redis.RedisComand;

public class PoolableSlicedRedis {
	final static int ReadWrite = 0;
	final static int ReadOnly = 2;
	final static int WriteOnly = 1;

	private static Logger logger = LoggerFactory
			.getLogger(PoolableSlicedRedis.class);

	private int sliceSize;
	private Plotter plotter = new HashPlotter();
	private Equalizer equalizer = new HashEqualizer();
	// private Plotter masterSlaveplotter = new HashPlotter();
	Map<Long, RedisSlice> poolables = new ConcurrentHashMap<>();

	/**
	 * <pre>
	 * hosts: MasterHost1:port[,Slavehost1-1:port,Slavehost1-2:port...] MasterHost2:port[,Slavehost2-1:port,Slavehost2-2:port...]
	 * 
	 * </pre>
	 * 
	 * @param hosts
	 * @param sliceSize
	 * @param plotter
	 * @param config
	 */
	public PoolableSlicedRedis(String hosts, int timeout, Equalizer equalizer,
			Plotter plotter, GenericObjectPool.Config config) {
		super();
		this.equalizer = equalizer;
		this.plotter = plotter;
		init(hosts, timeout, config);
	}

	public PoolableSlicedRedis(String hosts, int timeout, Equalizer equalizer,
			GenericObjectPool.Config config) {
		super();
		this.equalizer = equalizer;
		init(hosts, timeout, config);
	}

	public PoolableSlicedRedis(String hosts, int timeout, Plotter plotter,
			GenericObjectPool.Config config) {
		super();
		this.plotter = plotter;
		init(hosts, timeout, config);
	}

	/**
	 * default HashPlotter
	 * 
	 * @param hosts
	 * @param timeout
	 * @param config
	 */
	public PoolableSlicedRedis(String hosts, int timeout,
			GenericObjectPool.Config config) {
		super();
		init(hosts, timeout, config);
	}

	private void init(String hosts, int timeout, GenericObjectPool.Config config) {
		List<RedisSliceInfo> masters = new ArrayList<>();
		List<List<RedisSliceInfo>> allslaves = new ArrayList<>();
		String[] allhosts = hosts.split(" ");
		this.sliceSize = allhosts.length;
		for (String mshosts : allhosts) {
			String sliceHosts[] = mshosts.split(",");
			RedisSliceInfo master = toRedisSliceInfo(sliceHosts[0], timeout);
			masters.add(master);
			if (sliceHosts.length > 1) {
				List<RedisSliceInfo> sliceSlaves = new ArrayList<>();
				for (int i = 1; i < sliceHosts.length; i++) {
					RedisSliceInfo slave = toRedisSliceInfo(sliceHosts[i],
							timeout);
					sliceSlaves.add(slave);
				}
				allslaves.add(sliceSlaves);
			}

		}
		initRedisSlice(masters, allslaves, config);
		equalizer.mapSlice(poolables);

	}

	private void initRedisSlice(List<RedisSliceInfo> masters,
			List<List<RedisSliceInfo>> allslaves,
			GenericObjectPool.Config config) {
		for (int i = 0; i < masters.size(); i++) {
			RedisSliceInfo master = masters.get(i);
			if (allslaves != null && allslaves.size() > 0) {
				List<RedisSliceInfo> slaves = allslaves.get(i);

				RedisSlice redisSlice = new RedisSlice(master, slaves, plotter,
						config);
				poolables.put(Long.valueOf(i), redisSlice);
			} else {

				RedisSlice redisSlice = new RedisSlice(master,
						new RedisSliceInfo[] {}, plotter, config);
				poolables.put(Long.valueOf(i), redisSlice);
			}

		}

	}

	private RedisSliceInfo toRedisSliceInfo(String host, int timeout) {
		String[] hp = host.split(":");
		String h = hp[0];
		int p = Integer.parseInt(hp[1]);
		return new RedisSliceInfo(h, p, timeout);
	}

	public RedisComand createRedisCommand() {
		Class<RedisComand> iface = RedisComand.class;
		RedisComandsHandler handler = new RedisComandsHandler();

		return (RedisComand) Proxy.newProxyInstance(iface.getClassLoader(),
				new Class[] { iface }, handler);
	}

	public RedisComand createRedisCommand(int rw) {
		Class<RedisComand> iface = RedisComand.class;
		RedisComandsHandler handler = new RedisComandsHandler(rw);
		return (RedisComand) Proxy.newProxyInstance(iface.getClassLoader(),
				new Class[] { iface }, handler);
	}

	public void close() {
		Set<Entry<Long, RedisSlice>> pools = poolables.entrySet();
		for (Entry<Long, RedisSlice> entry : pools) {
			RedisSlice rs = entry.getValue();
			try {
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	Random random = new Random(19800202);

	private class RedisComandsHandler implements InvocationHandler {
		int readWrite = ReadWrite;

		public RedisComandsHandler() {

		}

		public RedisComandsHandler(int readWrite) {
			super();
			this.readWrite = readWrite;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			ObjectPool<Jedis> pool = null;
			Jedis jedis = null;
			try {
				byte[] key = null;

				Class<?> argsClass[] = method.getParameterTypes();
				if (args != null && args.length > 0) {
					// argsClass = new Class<?>[args.length];
					// for (int i = 0; i < args.length; i++) {
					// argsClass[i] = args[i].getClass();
					// System.out.println(argsClass[i].isPrimitive());
					// }

					Object obj = args[0];
					if (obj instanceof byte[]) {
						key = (byte[]) obj;
					} else {
						key = obj.toString().getBytes();
					}
				} else {
					// argsClass = new Class<?>[] {};
					key = String.valueOf(random.nextLong()).getBytes();
				}
				RedisSlice redisSlice = equalizer.get(new String(key),
						sliceSize);
				if (redisSlice == null) {
					throw new Exception("can't find slice.");
				}
				// System.out.println("index: " + index);
				// RedisSlice redisSlice = poolables.get(index);
				switch (readWrite) {
				case ReadWrite:
					pool = redisSlice.getAny(key);
					break;
				case ReadOnly:
					pool = redisSlice.getNextSlave(key);
					break;
				case WriteOnly:
					pool = redisSlice.getMaster(key);
					break;

				default:
					break;
				}

				jedis = pool.borrowObject();
				if (jedis == null) {
					throw new Exception("can't borrow jedis from pool");
				}
				Method origin = Jedis.class.getMethod(method.getName(),
						argsClass);
				Object obj = origin.invoke(jedis, args);
				return obj;
			} catch (Throwable e) {
				logger.error("Can not operate redis ", e);
				throw e;

			} finally {
				pool.returnObject(jedis);
			}
		}
	}

	public static interface RetryCallback {

		void execute() throws Exception;
	}

}
