package tech.dnaco.storage;

import tech.dnaco.storage.entity.StorageEntity;

public interface StorageTable<T extends StorageEntity> {
  void insert(StorageEntity entity);
  void update(StorageEntity entity);
  void upsert(StorageEntity entity);
  void delete(StorageEntity entity);
}