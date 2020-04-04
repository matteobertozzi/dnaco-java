package tech.dnaco.storage;

import tech.dnaco.storage.entity.StorageEntity;

public interface StorageReadOnlyTransaction extends AutoCloseable {
  @Override void close();

  <T extends StorageEntity> T get(String key, Class<T> classOfT) throws StorageException;
}
