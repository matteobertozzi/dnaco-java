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

package tech.dnaco.collections.arrays;

import java.util.Arrays;
import java.util.function.LongConsumer;

public class LongArray {
  public static final long[] EMPTY_ARRAY = new long[0];

  private long[] items;
  private int count;

  public LongArray(final int initialCapacity) {
    this.items = new long[initialCapacity];
    this.count = 0;
  }

  public void reset() {
    this.count = 0;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public boolean isNotEmpty() {
    return count != 0;
  }

  public int size() {
    return count;
  }

  public long[] rawBuffer() {
    return items;
  }

  public long[] buffer() {
    return Arrays.copyOf(items, count);
  }

  public long[] drain() {
    final long[] result;
    if (items.length == count) {
      result = items;
      this.items = EMPTY_ARRAY;
    } else if (count == 0) {
      result = EMPTY_ARRAY;
    } else {
      result = Arrays.copyOf(items, count);
    }
    this.count = 0;
    return result;
  }

  public long get(final int index) {
    return items[index];
  }

  public void set(final int index, final long value) {
    items[index] = value;
  }

  public void add(final long value) {
    if (count == items.length) {
      this.items = Arrays.copyOf(items, count + 16);
    }
    items[count++] = value;
  }

  public void add(final long[] value) {
    add(value, 0, value.length);
  }

  public void add(final long[] value, final int off, final int len) {
    if ((count + len) >= items.length) {
      this.items = Arrays.copyOf(items, count + len + 16);
    }
    System.arraycopy(value, off, items, count, len);
    count += len;
  }

  public void insert(final int index, final long value) {
    if (index == count) {
      if (count == items.length) {
        this.items = Arrays.copyOf(items, count + 16);
      }
      count++;
    }
    items[index] = value;
  }

  public void fill(final long value) {
    Arrays.fill(items, value);
  }

  public int indexOf(final int offset, final long value) {
    return ArrayUtil.indexOf(items, 0, count, value);
  }

  @Override
  public String toString() {
    return "LongArray [count=" + count + ", items=" + Arrays.toString(items) + "]";
  }

  public void forEach(final LongConsumer consumer) {
    for (int i = 0; i < count; ++i) {
      consumer.accept(items[i]);
    }
  }
}
