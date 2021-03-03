package tech.dnaco.storage.demo.logic;

import java.util.concurrent.ConcurrentHashMap;

import com.gullivernet.commons.collections.IdLock;

import tech.dnaco.storage.demo.driver.RocksDbKvStore;

public final class Storage {
  private static final ConcurrentHashMap<String, StorageLogic> projects = new ConcurrentHashMap<>();
  private static final IdLock projectLocks = new IdLock();

  private Storage() {
    // no-op
  }

  public static StorageLogic getInstance(final String projectId) throws Exception {
    final StorageLogic storage = projects.get(projectId);
    if (storage != null) return storage;

    try (IdLock.IdLockEntry lock = projectLocks.getLockEntry(projectId)) {
      StorageLogic newStorage = projects.get(projectId);
      if (newStorage != null) return newStorage;

      final RocksDbKvStore kvStorage = new RocksDbKvStore(projectId);
      //final MemoryKvStore kvStorage = new MemoryKvStore(projectId);
      kvStorage.loadStorage();

      newStorage = new StorageLogic(kvStorage);
      projects.put(projectId, newStorage);
      return newStorage;
    }
  }

  public static void shutdown() {
    for (final StorageLogic storage: projects.values()) {
      storage.shutdown();
    }
  }
}
