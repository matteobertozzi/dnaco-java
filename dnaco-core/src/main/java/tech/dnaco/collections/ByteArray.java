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

public class ByteArray {
  private byte[] blob;
  private int count;

  public ByteArray(final int initialCapacity) {
    this.blob = new byte[initialCapacity];
    this.count = 0;
  }

  public byte[] rawBuffer() {
    return blob;
  }

  public byte[] buffer() {
    if (count == blob.length) {
      return blob;
    }
    return Arrays.copyOf(blob, count);
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

  public long get(final int index) {
    return blob[index];
  }

  public void set(final int index, final int value) {
    blob[index] = (byte) (value & 0xff);
  }

  public void add(final int value) {
    if (count == blob.length) {
      this.blob = Arrays.copyOf(blob, count + 16);
    }
    blob[count++] = (byte) (value & 0xff);
  }

  public void add(final byte[] value) {
    add(value, 0, value.length);
  }

  public void add(final byte[] value, final int off, final int len) {
    if ((count + len) >= blob.length) {
      this.blob = Arrays.copyOf(blob, count + len + 16);
    }
    System.arraycopy(value, off, blob, count, len);
    count += len;
  }

  public void insert(final int index, final int value) {
    if (index == count) {
      if (count == blob.length) {
        this.blob = Arrays.copyOf(blob, count + 16);
      }
      count++;
    }
    blob[index] = (byte) (value & 0xff);
  }

  public void fill(final byte value) {
    Arrays.fill(blob, value);
  }

  public int indexOf(final int offset, final byte value) {
    return ArrayUtil.indexOf(blob, 0, count, value);
  }

  @Override
  public String toString() {
    return "ByteArray [count=" + count + ", items=" + Arrays.toString(blob) + "]";
  }
}