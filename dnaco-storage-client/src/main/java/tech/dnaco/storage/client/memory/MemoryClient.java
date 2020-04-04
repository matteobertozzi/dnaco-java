package tech.dnaco.storage.client.memory;

import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import tech.dnaco.storage.StorageException;
import tech.dnaco.storage.StorageReadOnlyTransaction;
import tech.dnaco.storage.StorageTransaction;
import tech.dnaco.storage.entity.StorageEntity;

public class MemoryClient {
  private final HashMap<String, AtomicLong> counters = new HashMap<>();
  private final TreeMap<String, String> globalData = new TreeMap<>();

  public long incrementAndGet(final String tenantId, final String path) {
		return counters.computeIfAbsent(tenantId + path, (k) -> new AtomicLong(0)).incrementAndGet();
  }

  public long addAndGet(final String tenantId, final String path, final long delta) {
		return counters.computeIfAbsent(tenantId + path, (k) -> new AtomicLong(0)).addAndGet(delta);
	}

  public StorageTransaction newTransaction(final String tenantId) {
    return new ReadWriteTransaction(globalData, tenantId);
  }

  public StorageReadOnlyTransaction newReadOnlyTransaction(final String tenantId) {
    return new ReadOnlyTransaction(globalData, tenantId);
  }

  private static class ReadWriteTransaction implements StorageTransaction {
    private final MemoryDeltaStorage storage;
    private final String tenantId;

    private ReadWriteTransaction(final NavigableMap<String, String> globalData, final String tenantId) {
      this.storage = new MemoryDeltaStorage(globalData);
      this.tenantId = tenantId;
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public <T extends StorageEntity> T get(final String key, final Class<T> classOfT) throws StorageException {
      return storage.get(tenantId, key, classOfT);
    }

    @Override
    public void insert(final String key, final Object value) throws StorageException {
      storage.insert(tenantId, key, value);
    }

    @Override
    public void update(final String key, final Object value) throws StorageException {
      storage.update(tenantId, key, value);
    }

    @Override
    public void upsert(final String key, final Object value) throws StorageException {
      storage.upsert(tenantId, key, value);
    }

    @Override
    public void delete(final String key) throws StorageException {
      storage.delete(tenantId, key);
    }

    @Override
    public void commit() throws StorageException {
      storage.commit();
    }

    @Override
    public void rollback() {
      // no-op
    }
  }

  private static class ReadOnlyTransaction implements StorageReadOnlyTransaction {
    private final MemoryDeltaStorage storage;
    private final String tenantId;

    private ReadOnlyTransaction(final NavigableMap<String, String> globalData, final String tenantId) {
      this.storage = new MemoryDeltaStorage(globalData);
      this.tenantId = tenantId;
	  }

    @Override
    public void close() {
      // no-op
    }

	  @Override
    public <T extends StorageEntity> T get(final String key, final Class<T> classOfT) {
      return storage.get(tenantId, key, classOfT);
    }
  }
}