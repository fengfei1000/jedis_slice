package fengfei.redis.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public interface Hashed {

	public static final Hashed MURMUR_HASH = new MurmurHashed();
	public ThreadLocal<MessageDigest> md5Holder = new ThreadLocal<MessageDigest>();

	public static final Hashed MD5 = new Hashed() {

		public long hash(String key) {
			return hash(key.getBytes());
		}

		public long hash(byte[] key) {
			try {
				if (md5Holder.get() == null) {
					md5Holder.set(MessageDigest.getInstance("MD5"));
				}
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException("++++ no md5 algorythm found");
			}
			MessageDigest md5 = md5Holder.get();

			md5.reset();
			md5.update(key);
			byte[] bKey = md5.digest();
			long res = ((long) (bKey[3] & 0xFF) << 24)
					| ((long) (bKey[2] & 0xFF) << 16)
					| ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
			return res;
		}

		@Override
		public int hash32(String key) {
			return hash32(key.getBytes());
		}

		@Override
		public int hash32(byte[] key) {
			try {
				if (md5Holder.get() == null) {
					md5Holder.set(MessageDigest.getInstance("MD5"));
				}
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException("++++ no md5 algorythm found");
			}
			MessageDigest md5 = md5Holder.get();

			md5.reset();
			md5.update(key);
			byte[] bKey = md5.digest();
			int res = (  (bKey[3] & 0xFF) << 24)
					| (  (bKey[2] & 0xFF) << 16)
					| (  (bKey[1] & 0xFF) << 8) |   (bKey[0] & 0xFF);
			return res;
		}
	};

	public long hash(String key);

	public long hash(byte[] key);

	public int hash32(String key);

	public int hash32(byte[] key);
}