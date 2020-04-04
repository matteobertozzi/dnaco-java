package tech.dnaco.storage.client;

import tech.dnaco.storage.StorageCounter;
import tech.dnaco.storage.StorageUtil;

public abstract class AbstractStorageCounter implements StorageCounter {
	@Override
	public long incrementAndGet(final String tenantId, final String key) {
		return incrementAndGet(StorageUtil.toByteBuf(tenantId), StorageUtil.toByteBuf(key));
	}

	@Override
	public long incrementAndGet(final byte[] tenantId, final byte[] key) {
		return incrementAndGet(StorageUtil.toByteBuf(tenantId), StorageUtil.toByteBuf(key));
	}

	@Override
	public long addAndGet(final String tenantId, final String key, final long delta) {
		return addAndGet(StorageUtil.toByteBuf(tenantId), StorageUtil.toByteBuf(key), delta);
	}

	@Override
	public long addAndGet(final byte[] tenantId, final byte[] key, final long delta) {
		return addAndGet(StorageUtil.toByteBuf(tenantId), StorageUtil.toByteBuf(key), delta);
	}
}