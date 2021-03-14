package tech.dnaco.collections.maps;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.LongFunction;

import tech.dnaco.collections.Hashing;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.iterators.AbstractIndexIterator;
import tech.dnaco.util.BitUtil;

public class LongToObjectMap<V> implements Iterable<LongToObjectMap.Entry<V>> {
  private static final float DEFAULT_LOAD_FACTOR = 0.55f;
  private static final int MIN_CAPACITY = 16;

  private final float loadFactor;

  private long[] keys;
  private Object[] values;
  private int size;
  private int resizeThreshold;

  public LongToObjectMap() {
    this(MIN_CAPACITY);
  }

  public LongToObjectMap(final int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public LongToObjectMap(final int initialCapacity, final float loadFactor) {
    final int capacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, initialCapacity));
    this.loadFactor = loadFactor;
    this.resizeThreshold = (int) (capacity * loadFactor);

    this.keys = new long[capacity];
    this.values = new Object[capacity];
  }

  // ==========================================================================================
  //  Utility methods
  // ==========================================================================================
  public boolean isEmpty() {
    return size == 0;
  }

  public int size() {
    return this.size;
  }

  public int capacity() {
    return keys.length;
  }

  public int idealCapacity() {
    return Math.round(size * (1.0f / loadFactor));
  }

  public void clear() {
    Arrays.fill(values, null);
    size = 0;
  }

  // ==========================================================================================
  // Get/Contains related
  // ==========================================================================================
  public V get(final long key) {
    return ArrayUtil.getItemAt(values, getIndex(key));
  }

  public V getOrDefault(final long key, final V defaultValue) {
    final V v = get(key);
    return (v != null) ? v : defaultValue;
  }

  public boolean containsKey(final long key) {
    return values[getIndex(key)] != null;
  }

  private int getIndex(final long key) {
    final int mask = values.length - 1;
    int index = Hashing.hash(key, mask);
    while (values[index] != null) {
      if (keys[index] == key) {
        break;
      }
      index = ++index & mask;
    }
    return index;
  }

  // ==========================================================================================
  // Remove related
  // ==========================================================================================
  public V remove(final long key) {
    final int mask = values.length - 1;
    int index = Hashing.hash(key, mask);

    V value;
    while ((value = ArrayUtil.getItemAt(values, index)) != null) {
      if (key == keys[index]) {
        values[index] = null;
        --size;
        // TODO: rehash?
        if (size * 5 < resizeThreshold) {
          compact();
        }
        return value;
      }
      index = ++index & mask;
    }
    return null;
  }

  public void compact() {
    final int newCapacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, idealCapacity()));
    if (newCapacity != keys.length) resize(newCapacity);
  }

  // ==========================================================================================
  // Put related
  // ==========================================================================================
  public V computeIfAbsent(final long key, final LongFunction<? extends V> mappingFunction) {
    Objects.requireNonNull(mappingFunction);
    V v;
    if ((v = get(key)) == null) {
      V newValue;
      if ((newValue = mappingFunction.apply(key)) != null) {
        put(key, newValue);
        return newValue;
      }
    }
    return v;
  }

  public V put(final long key, final V value) {
    return put(key, value, false);
  }

  public V putIfAbsent(final long key, final V value) {
    return put(key, value, true);
  }

  private V put(final long key, final V value, final boolean ifAbsent) {
    final int mask = values.length - 1;
    int index = Hashing.hash(key, mask);

    V oldValue = null;
    while (values[index] != null) {
      if (key == keys[index]) {
        oldValue = ArrayUtil.getItemAt(values, index);
        break;
      }
      index = ++index & mask;
    }

    if (oldValue == null) {
      keys[index] = key;
      values[index] = value;
      if (++size > resizeThreshold) {
        grow();
      }
    } else if (!ifAbsent) {
      values[index] = value;
    }

    return oldValue;
  }

  private void grow() {
    final int newCapacity = values.length << 1;
    if (newCapacity < 0) {
      throw new IllegalStateException("HashMap too big size=" + size);
    }
    resize(newCapacity);
  }

  private void resize(final int newCapacity) {
    final int mask = newCapacity - 1;
    this.resizeThreshold = (int) (newCapacity * loadFactor);

    final long[] tempKeys = new long[newCapacity];
    final Object[] tempValues = new Object[newCapacity];

    for (int i = 0, size = values.length; i < size; i++) {
      final V value = ArrayUtil.getItemAt(values, i);
      if (value != null) {
        final long key = keys[i];
        int newHash = Hashing.hash(key, mask);
        while (tempValues[newHash] != null) {
          newHash = ++newHash & mask;
        }

        tempKeys[newHash] = key;
        tempValues[newHash] = value;
      }
    }

    this.keys = tempKeys;
    this.values = tempValues;
  }

  // ==========================================================================================
  // Iterator related
  // ==========================================================================================
  @Override
  public Iterator<Entry<V>> iterator() {
    return new EntryIterator<>(this);
  }

  public long[] keySet() {
    final long[] keySet = new long[size];
    for (int i = 0, count = 0; i < values.length; ++i) {
      if (values[i] != null) {
        keySet[count++] = keys[i];
      }
    }
    return keySet;
  }

  public V[] values() {
    return ArrayUtil.copyNotNull(values, size);
  }

  public static final class Entry<V> {
    private long key;
    private V value;

    public long getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    private Entry<V> set(final long key, final V value) {
      this.key = key;
      this.value = value;
      return this;
    }
  }

  private static final class EntryIterator<V> extends AbstractIndexIterator<Entry<V>> {
    private final Entry<V> entry = new Entry<>();
    private final LongToObjectMap<V> map;

    public EntryIterator(final LongToObjectMap<V> map) {
      super(map.keys.length);
      this.map = map;
    }

    @Override
    protected Entry<V> nextItemAt(final int index) {
      return entry.set(map.keys[index], ArrayUtil.getItemAt(map.values, index));
    }

    @Override
    protected boolean isEmpty(final int index) {
      return map.values[index] == null;
    }
  }

  // ==========================================================================================
  // ToString
  // ==========================================================================================
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("{");
    for (int i = 0, count = 0; i < values.length; ++i) {
      if (values[i] == null) continue;

      if (count++ > 0) builder.append(", ");
      builder.append(keys[i]);
      builder.append(": ");
      builder.append(values[i]);
    }
    builder.append("}");
    return builder.toString();
  }
}
