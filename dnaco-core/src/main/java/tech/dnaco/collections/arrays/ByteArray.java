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

import tech.dnaco.bytes.ByteArrayAppender;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;

public class ByteArray implements ByteArrayAppender {
  public static final byte[] EMPTY_ARRAY = BytesUtil.EMPTY_BYTES;
  private static final int EXTRA_SIZE = 1024;

  private byte[] blob;
  private int count;

  public ByteArray(final int initialCapacity) {
    this.blob = new byte[initialCapacity];
    this.count = 0;
  }

  public ByteArray(final byte[] blob) {
    this.blob = blob;
    this.count = blob.length;
  }

  public void truncate(final int offset) {
    this.count = offset;
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

  public byte[] rawBuffer() {
    return blob;
  }

  public byte[] buffer() {
    return Arrays.copyOf(blob, count);
  }

  public byte[] drain() {
    final byte[] result;
    if (blob.length == count) {
      result = blob;
      this.blob = EMPTY_ARRAY;
    } else if (count == 0) {
      result = EMPTY_ARRAY;
    } else {
      result = Arrays.copyOf(blob, count);
    }
    this.count = 0;
    return result;
  }

  public ByteArraySlice slice() {
    return new ByteArraySlice(blob, 0, count);
  }

  public byte get(final int index) {
    return blob[index];
  }

  public void set(final int index, final int value) {
    blob[index] = (byte) (value & 0xff);
  }

  public void set(final byte[] value) {
    this.blob = value;
    this.count = value.length;
  }

  @Override
  public void add(final int value) {
    if (count == blob.length) {
      this.blob = Arrays.copyOf(blob, count + EXTRA_SIZE);
    }
    blob[count++] = (byte) (value & 0xff);
  }

  @Override
  public void add(final byte[] value) {
    add(value, 0, value.length);
  }

  @Override
  public void add(final byte[] value, final int off, final int len) {
    if ((count + len) >= blob.length) {
      this.blob = Arrays.copyOf(blob, count + len + EXTRA_SIZE);
    }
    System.arraycopy(value, off, blob, count, len);
    count += len;
  }

  public void insert(final int index, final int value) {
    if (index == count) {
      if (count == blob.length) {
        this.blob = Arrays.copyOf(blob, count + EXTRA_SIZE);
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
  public int hashCode() {
    if (count == 0) return 0;

    int result = 1;
    for (int i = 0; i < count; ++i) {
      result = 31 * result + blob[i];
    }
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof final ByteArray other)) return false;

    if (count != other.count) return false;

    return Arrays.equals(blob, 0, count, other.blob, 0, count);
  }

  @Override
  public String toString() {
    return "ByteArray [count=" + count + ", items=" + Arrays.toString(blob) + "]";
  }

  public void addFixed(final int bytesWidth, final long value) {
    for (int i = 0; i < bytesWidth; ++i) {
      //add((int)((value >>> (i << 3)) & 0xff));
    }
  }

  public void addFixed32(final long value) {
    addFixed(4, value);
  }

  public void addFixed64(final long value) {
    addFixed(8, value);
  }
}
