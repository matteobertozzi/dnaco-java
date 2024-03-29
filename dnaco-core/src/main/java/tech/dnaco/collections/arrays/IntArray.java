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

public class IntArray {
  public static final int[] EMPTY_ARRAY = new int[0];

  private int[] items;
  private int count;

  public IntArray(final int initialCapacity) {
    this.items = new int[initialCapacity];
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

  public int[] rawBuffer() {
    return items;
  }

  public int[] buffer() {
    return Arrays.copyOf(items, count);
  }

  public int[] drain() {
    final int[] result;
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

  public int get(final int index) {
    return items[index];
  }

  public void set(final int index, final int value) {
    items[index] = value;
  }

  public void add(final int value) {
    if (count == items.length) {
      this.items = Arrays.copyOf(items, count + 16);
    }
    items[count++] = value;
  }

  public void add(final int[] value) {
    add(value, 0, value.length);
  }

  public void add(final int[] value, final int off, final int len) {
    if ((count + len) >= items.length) {
      this.items = Arrays.copyOf(items, count + len + 16);
    }
    System.arraycopy(value, off, items, count, len);
    count += len;
  }

  public void insert(final int index, final int value) {
    if (index == count) {
      if (count == items.length) {
        this.items = Arrays.copyOf(items, count + 16);
      }
      count++;
    }
    items[index] = value;
  }

  public void swap(final int aIndex, final int bIndex) {
    ArrayUtil.swap(items, aIndex, bIndex);
  }

  public void fill(final int value) {
    Arrays.fill(items, value);
  }

  public int indexOf(final int offset, final int value) {
    return ArrayUtil.indexOf(items, 0, count, value);
  }

  @Override
  public String toString() {
    return "IntArray [count=" + count + ", items=" + Arrays.toString(items) + "]";
  }
}
