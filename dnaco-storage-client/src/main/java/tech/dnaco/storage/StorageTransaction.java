package tech.dnaco.storage;

public interface StorageTransaction extends StorageReadOnlyTransaction {
  void insert(String key, Object value) throws StorageException;
  void update(String key, Object value) throws StorageException;
  void upsert(String key, Object value) throws StorageException;
  void delete(String key) throws StorageException;

  void commit() throws StorageException;
  void rollback();
}
