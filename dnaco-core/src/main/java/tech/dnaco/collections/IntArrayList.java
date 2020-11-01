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

public final class IntArrayList {
  private int[] items;
  private int count;

  public IntArrayList(final int initialCapacity) {
    this.items = new int[initialCapacity];
    this.count = 0;
  }

  public void resize(final int initialCapacity) {
    if (initialCapacity <= items.length) return;
    this.items = Arrays.copyOf(items, initialCapacity);
  }

  public int[] toArray() {
    return Arrays.copyOf(items, count);
  }

  public int[] rawBuffer() {
    return items;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public boolean isNotEmpty() {
    return count != 0;
  }

  public void reset() {
    this.count = 0;
  }

  public int size() {
    return count;
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

  public void insert(final int index, final int value) {
    if (index == count) {
      if (count == items.length) {
        this.items = Arrays.copyOf(items, count + 16);
      }
    }
    ArrayUtil.insert(items, 0, count, index, value);
    count++;
  }

  public void fill(final int value) {
    Arrays.fill(items, value);
  }

  public int indexOf(final int offset, final int value) {
    return ArrayUtil.indexOf(items, 0, count, value);
  }

  public int binarySearch(final int value) {
    return Arrays.binarySearch(items, 0, count, value);
  }

  @Override
  public String toString() {
    return "IntArrayList [count=" + count + ", items=" + Arrays.toString(items) + "]";
  }
}
