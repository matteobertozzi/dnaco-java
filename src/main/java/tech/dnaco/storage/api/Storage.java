package tech.dnaco.storage.api;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.gullivernet.commons.collections.IdLock;
import com.gullivernet.commons.collections.IdLock.IdLockEntry;

import tech.dnaco.storage.StorageConfig;
import tech.dnaco.storage.blocks.BlockEntry;
import tech.dnaco.storage.blocks.BlockManager;
import tech.dnaco.storage.memstore.MemStore;

public final class Storage {
  private static final ConcurrentHashMap<String, Storage> projectStorage = new ConcurrentHashMap<>(128);
  private static final IdLock projectLock = new IdLock();

  public static Storage getInstance(final String projectId) throws IOException {
    try (IdLockEntry lock = projectLock.getLockEntry(projectId)) {
      final Storage storage = projectStorage.get(projectId);
      if (storage != null) return storage;

      final Storage newStorage = new Storage(projectId);
      final Storage oldStorage = projectStorage.putIfAbsent(projectId, newStorage);
      if (oldStorage != null) return oldStorage;

      newStorage.init();
      return newStorage;
    }
  }

  private final BlockManager blockManager;
  private final MemStore memStore;

  private Storage(final String projectId) {
    this.blockManager = new BlockManager(StorageConfig.INSTANCE.getBlocksStorageDir(projectId));
    this.memStore = this.blockManager.addMemStore(new MemStore());
  }

  public void init() throws IOException {
    this.blockManager.loadBlockIndex();
  }

  public BlockManager getBlockManager() {
    return blockManager;
  }

  public void addAll(final List<BlockEntry> entries) {
    this.memStore.addAll(entries);
  }

  public void flush() throws IOException {
    this.memStore.flush(blockManager);
  }
}
