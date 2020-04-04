package tech.dnaco.storage;

public interface StorageKeyValue {
  Object get(String key);

  void insert(String key, Object value);
  void update(String key, Object value);
  void upsert(String key, Object value);
  void delete(String key);
}