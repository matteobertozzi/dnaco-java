package tech.dnaco.storage.client.memory;

import java.util.NavigableMap;
import java.util.TreeMap;

import tech.dnaco.storage.StorageException;
import tech.dnaco.storage.StorageKeyExistsException;
import tech.dnaco.storage.StorageKeyNotFoundException;
import tech.dnaco.storage.entity.StorageEntity;
import tech.dnaco.util.JsonUtil;

public class MemoryDeltaStorage {
  private final NavigableMap<String, String> globalData;
  private final NavigableMap<String, KeyValue> data;

  public MemoryDeltaStorage(final NavigableMap<String, String> globalData) {
    this.data = new TreeMap<>();
    this.globalData = globalData;
  }

  private String keyPath(final String tenantId, final String key) {
    return tenantId + key;
  }

  private String encodeEntity(final Object value) {
    return JsonUtil.toJson(value);
  }

  private <T extends StorageEntity> T decodeEntity(final String value, final Class<T> classOfT) {
    return JsonUtil.fromJson(value, classOfT);
  }

  public void commit() throws StorageException {
    for (final KeyValue kv: data.values()) {
      if (kv.state == KeyValueState.INSERT) {
        if (globalData.containsKey(kv.key)) {
          throw new StorageKeyExistsException(kv.key);
        }
      } else if (kv.state == KeyValueState.UPDATE) {
        if (!globalData.containsKey(kv.key)) {
          throw new StorageKeyNotFoundException(kv.key);
        }
      }
    }

    for (final KeyValue kv: data.values()) {
      if (kv.state == KeyValueState.DELETE) {
        globalData.remove(kv.key);
        System.out.println(" -> RM " + kv.key);
      } else {
        globalData.put(kv.key, kv.value);
        System.out.println(" -> ADD " + kv.key + " -> " + kv.value);
      }
    }
    System.out.println(" -> " + globalData);
  }

  public <T extends StorageEntity> T get(final String tenantId, final String key, final Class<T> classOfT) {
    final String path = keyPath(tenantId, key);

    final KeyValue kv = data.get(path);
    if (kv != null) {
      if (kv.state == KeyValueState.DELETE) {
        return null;
      }
      return decodeEntity(kv.value, classOfT);
    }

    return decodeEntity(globalData.get(path), classOfT);
  }

  public void insert(final String tenantId, final String key, final Object value) throws StorageKeyExistsException {
    final String path = keyPath(tenantId, key);
    if (data.putIfAbsent(path, new KeyValue(KeyValueState.INSERT, path, encodeEntity(value))) != null) {
      throw new StorageKeyExistsException(path);
    }
  }

  public void update(final String tenantId, final String key, final Object value) throws StorageKeyNotFoundException {
    final String path = keyPath(tenantId, key);
    final KeyValue kv = data.get(path);
    if (kv != null) {
      if (kv.state == KeyValueState.DELETE) {
        throw new StorageKeyNotFoundException(kv.key);
      }
      kv.value = encodeEntity(value);
    } else {
      data.put(path, new KeyValue(KeyValueState.UPDATE, path, encodeEntity(value)));
    }
  }

  public void upsert(final String tenantId, final String key, final Object value) {
    final String path = keyPath(tenantId, key);
    data.put(path, new KeyValue(KeyValueState.UPSERT, path, encodeEntity(value)));
  }

  public void delete(final String tenantId, final String key) {
    final String path = keyPath(tenantId, key);
    data.put(path, new KeyValue(KeyValueState.DELETE, path, null));
  }

  public enum KeyValueState { INSERT, UPDATE, UPSERT, DELETE }
  private static final class KeyValue {
    private final String key;
    private String value;
    private final KeyValueState state;

    public KeyValue(final KeyValueState state, final String key, final String value) {
      this.key = key;
      this.value = value;
      this.state = state;
    }
  }
}