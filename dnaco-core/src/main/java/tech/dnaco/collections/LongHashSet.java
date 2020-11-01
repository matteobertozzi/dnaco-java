/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.collections;

import java.util.Arrays;

import tech.dnaco.util.BitUtil;

public class LongHashSet {
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final int MIN_CAPACITY = 16;

  private final float loadFactor;
  private final int missingValue;

  private long[] keys;
  private int size;
  private int resizeThreshold;

  public LongHashSet(final int missingValue) {
    this(missingValue, MIN_CAPACITY);
  }

  public LongHashSet(final int missingValue, final int initialCapacity) {
    this(missingValue, initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public LongHashSet(final int missingValue, final int initialCapacity, final float loadFactor) {
    final int capacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, initialCapacity));
    this.loadFactor = loadFactor;
    this.resizeThreshold = (int) (capacity * loadFactor);
    this.missingValue = missingValue;

    this.keys = new long[capacity];
    Arrays.fill(this.keys, missingValue);
  }

  public static LongHashSet fromArray(final int missingValue, final long[] values) {
    final LongHashSet hashSet = new LongHashSet(missingValue, Math.round(values.length * 1.5f));
    hashSet.addAll(values);
    return hashSet;
  }

  // ==========================================================================================
  //  Utility methods
  // ==========================================================================================
  public boolean isEmpty() {
    return size == 0;
  }

  public boolean isNotEmpty() {
    return size != 0;
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
    Arrays.fill(keys, missingValue);
    size = 0;
  }

  // ==========================================================================================
  //  Hash related
  // ==========================================================================================
  private static int hash(final long value, final int mask) {
    long hash = value * 31;
    hash = (int) hash ^ (int) (hash >>> 32);
    return (int) hash & mask;
  }

  // ==========================================================================================
  // Get/Contains related
  // ==========================================================================================
  public boolean contains(final long key) {
    final int mask = keys.length - 1;
    int index = hash(key, mask);
    while (keys[index] != missingValue) {
      if (keys[index] == key) {
        return true;
      }
      index = ++index & mask;
    }
    return false;
  }

  // ==========================================================================================
  // Remove related
  // ==========================================================================================
  public boolean remove(final long key) {
    final int mask = keys.length - 1;
    int index = hash(key, mask);

    while (keys[index] != missingValue) {
      if (key == keys[index]) {
        keys[index] = missingValue;
        --size;
        // TODO: rehash?
        if (size * 5 < resizeThreshold) {
          compact();
        }
        return true;
      }
      index = ++index & mask;
    }
    return false;
  }

  public void compact() {
    final int newCapacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, idealCapacity()));
    if (newCapacity != keys.length) resize(newCapacity);
  }

  // ==========================================================================================
  // Add related
  // ==========================================================================================
  public boolean add(final long key) {
    if (key == missingValue) {
      throw new IllegalArgumentException(missingValue + " cannot be used as key");
    }

    final int mask = keys.length - 1;
    int index = hash(key, mask);

    long oldValue = missingValue;
    while (keys[index] != missingValue) {
      if (key == keys[index]) {
        oldValue = keys[index];
        break;
      }
      index = ++index & mask;
    }

    if (oldValue == missingValue) {
      keys[index] = key;
      if (++size > resizeThreshold) {
        grow();
      }
    }

    return oldValue == missingValue;
  }

  public int addAll(final long[] values) {
    int count = 0;
    for (int i = 0; i < values.length; ++i) {
      count += add(values[i]) ? 1 : 0;
    }
    return count;
  }

  private void grow() {
    final int newCapacity = keys.length << 1;
    if (newCapacity < 0) {
      throw new IllegalStateException("HashMap too big size=" + size);
    }
    resize(newCapacity);
  }

  private void resize(final int newCapacity) {
    final int mask = newCapacity - 1;
    this.resizeThreshold = (int) (newCapacity * loadFactor);

    final long[] tempKeys = new long[newCapacity];
    Arrays.fill(tempKeys, missingValue);
    for (int i = 0, size = keys.length; i < size; i++) {
      if (keys[i] != missingValue) {
        final long key = keys[i];
        int newHash = hash(key, mask);
        while (tempKeys[newHash] != missingValue) {
          newHash = ++newHash & mask;
        }

        tempKeys[newHash] = key;
      }
    }

    this.keys = tempKeys;
  }

  // ==========================================================================================
  // Iterator related
  // ==========================================================================================
  public long[] keySet() {
    final long[] keySet = new long[size];
    for (int i = 0, count = 0; i < keys.length; ++i) {
      if (keys[i] != missingValue) {
        keySet[count++] = keys[i];
      }
    }
    return keySet;
  }

  // ==========================================================================================
  // ToString
  // ==========================================================================================
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("{");
    for (int i = 0, count = 0; i < keys.length; ++i) {
      if (keys[i] == missingValue) continue;

      if (count++ > 0) builder.append(", ");
      builder.append(keys[i]);
    }
    builder.append("}");
    return builder.toString();
  }
}
