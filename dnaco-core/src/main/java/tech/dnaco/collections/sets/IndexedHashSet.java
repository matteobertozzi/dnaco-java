package tech.dnaco.collections.sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.util.BitUtil;

public final class IndexedHashSet<K> {
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final int MIN_CAPACITY = 64;

  private static final class IndexedSetEntry {
    private int hash;
    private int next;
    private Object key;
  }

  private IndexedSetEntry[] entries;
  private int[] buckets;
  private int entriesIndex;
  private int freeList;
  private int count;

  public IndexedHashSet() {
    this(MIN_CAPACITY);
  }

  public IndexedHashSet(final int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public IndexedHashSet(final int initialCapacity, final float loadFactor) {
    if (loadFactor < 0.1f || loadFactor > 0.9f) {
      throw new IllegalArgumentException("invalid loadFactor " + loadFactor + ", must be [0.1, 0.9]");
    }

    final int capacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, initialCapacity));
    System.out.println(capacity);
    this.entriesIndex = 0;
    this.freeList = -1;
    this.entries = new IndexedSetEntry[capacity];
    this.buckets = new int[capacity];
    this.count = 0;

    Arrays.fill(buckets, -1);
  }


  public static void main(final String[] args) {
    final IndexedHashSet<String> index = new IndexedHashSet<>();
    index.addAll(List.of("__group__", "__op__", "__seqId__", "__ts__", "date", "id", "latitude", "longitude", "subtypes"));
    System.out.println();
    for (int i = 0; i < index.size(); ++i) {
      System.out.println(i + " -> " + index.get(index.get(i)));
    }
  }


  public int size() {
    return count;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public int get(final K key) {
    return findEntry(key);
  }

  @SuppressWarnings("unchecked")
  public K get(final int index) {
    return (K) entries[index].key;
  }

  public boolean containsKey(final K key) {
    return findEntry(key) >= 0;
  }

  private int findEntry(final K key) {
    final int hashCode = Objects.hashCode(key) & 0x7FFFFFFF;
    return findEntry(key, hashCode);
  }

  private int findEntry(final K key, final int hashCode) {
    for (int i = buckets[hashCode % buckets.length]; i >= 0; i = entries[i].next) {
      final IndexedSetEntry entry = entries[i];
      if (entry.hash == hashCode && Objects.equals(entry.key, key)) {
        return i;
      }
    }
    return -1;
  }

  public int add(final K key) {
    final int hashCode = Objects.hashCode(key) & 0x7FFFFFFF;
    final int index = findEntry(key, hashCode);
    if (index >= 0) return index;

    return insertNewEntry(hashCode, key);
  }

  private int insertNewEntry(final int hashCode, final K key) {
    int targetBucket = hashCode % buckets.length;
    final int index;
    if (freeList >= 0) {
      index = freeList;
      freeList = entries[index].next;
    } else {
      if (entriesIndex == entries.length) {
        resize();
        targetBucket = hashCode % buckets.length;
      }
      index = entriesIndex++;
    }

    writeEntry(index, hashCode, buckets[targetBucket], key);
    buckets[targetBucket] = index;
    count++;
    return index;
  }

  private void resize() {
    final int newCapacity = entriesIndex << 1;
    if (newCapacity < 0) {
      throw new IllegalStateException("HashMap too big size=" + entriesIndex);
    }
    resize(newCapacity);
  }

  private void resize(final int newSize) {
    final int[] newBuckets = new int[newSize];
    Arrays.fill(newBuckets, -1);

    final IndexedSetEntry[] newEntries = new IndexedSetEntry[newSize];
    System.arraycopy(entries, 0, newEntries, 0, entriesIndex);
    for (int i = 0; i < entriesIndex; i++) {
      if (newEntries[i].hash >= 0) {
        final int bucket = newEntries[i].hash % newSize;
        newEntries[i].next = newBuckets[bucket];
        newBuckets[bucket] = i;
      }
    }
    this.buckets = newBuckets;
    this.entries = newEntries;
  }

  public int remove(final K key) {
    final int hashCode = Objects.hashCode(key) & 0x7FFFFFFF;
    final int bucket = hashCode % buckets.length;
    int last = -1;
    for (int i = buckets[bucket]; i >= 0; last = i, i = entries[i].next) {
      final IndexedSetEntry entry = entries[i];
      if (entry.hash == hashCode && Objects.equals(entry.key, key)) {
        if (last < 0) {
          buckets[bucket] = entry.next;
        } else {
          entries[last].next = entry.next;
        }
        entry.hash = -1;
        entry.next = freeList;
        entry.key = null;
        freeList = i;
        count--;
        resize(entriesIndex);
        return i;
      }
    }
    return -1;
  }

  private void writeEntry(final int index, final int hashCode, final int next, final K key) {
    final IndexedSetEntry entry = getOrCreateEntry(index);
    entry.hash = hashCode;
    entry.next = next;
    entry.key = key;
  }

  private IndexedSetEntry getOrCreateEntry(final int index) {
    final IndexedSetEntry entry = entries[index];
    if (entry != null) return entry;

    entries[index] = new IndexedSetEntry();
    return entries[index];
  }

  public void addAll(final Collection<K> other) {
    if (ListUtil.isEmpty(other)) return;
    for (final K key: other) {
      add(key);
    }
  }

  public void addAll(final K[] other) {
    if (ArrayUtil.isEmpty(other)) return;
    for (int i = 0; i < other.length; ++i) {
      add(other[i]);
    }
  }

  public void clear() {
    Arrays.fill(buckets, -1);
    Arrays.fill(entries, null);
    this.freeList = -1;
    this.entriesIndex = 0;
  }

  public List<K> keys() {
    final ArrayList<K> keys = new ArrayList<>(count);
    for (int i = 0; i < entriesIndex; ++i) {
      if (entries[i] != null && entries[i].key != null) {
        keys.add(getEntryKey(entries[i]));
      }
    }
    return keys;
  }

  @SuppressWarnings("unchecked")
  private K getEntryKey(final IndexedSetEntry entry) {
    return (K) entry.key;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("{");
    int itemIndex = 0;
    for (int i = 0; i < entriesIndex; ++i) {
      if (entries[i] != null && entries[i].key != null) {
        if (itemIndex++ > 0) builder.append(", ");
        builder.append(entries[i].key);
      }
    }
    builder.append("}");
    return builder.toString();
  }
}

