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

package tech.dnaco.bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import tech.dnaco.collections.arrays.ArrayUtil.ByteArrayConsumer;

public class ByteArraySlice implements BytesSlice {
  private byte[] buf;
  private int off;
  private int len;

  public ByteArraySlice() {
    this(BytesUtil.EMPTY_BYTES);
  }

  public ByteArraySlice(final byte[] buf) {
    this(buf, 0, buf.length);
  }

  public ByteArraySlice(final byte[] buf, final int off, final int len) {
    set(buf, off, len);
  }

  public void set(final byte[] buf) {
    set(buf, 0, buf.length);
  }

  public void set(final byte[] buf, final int off, final int len) {
    this.buf = buf;
    this.off = off;
    this.len = len;
  }

  @Override
  public boolean isEmpty() {
    return len == 0;
  }

  @Override
  public boolean isNotEmpty() {
    return len != 0;
  }

  public byte[] rawBuffer() {
    return buf;
  }

  public byte[] buffer() {
    return Arrays.copyOfRange(buf, off, off + len);
  }

  public int offset() {
    return off;
  }

  @Override
  public int length() {
    return len;
  }

  @Override
  public int get(final int index) {
    if (index >= len) throw new IndexOutOfBoundsException();
    return buf[off + index] & 0xff;
  }

  @Override
  public void copyTo(final byte[] xBuf, final int xOff, final int xLen) {
    System.arraycopy(this.buf, this.off, xBuf, xOff, xLen);
  }

  @Override
  public void forEach(final ByteArrayConsumer consumer) {
    consumer.accept(buf, off, len);
  }

  @Override
  public void forEach(final int offset, final int length, final ByteArrayConsumer consumer) {
    if ((offset + length) > len) throw new IndexOutOfBoundsException();
    consumer.accept(buf, off + offset, length);
  }

  public void writeTo(final OutputStream stream) throws IOException {
    stream.write(buf, off, len);
  }

  @Override
  public String toString() {
    return "BytesSlice [off=" + off + ", len=" + len + ", buf=" + new String(buf, off, len) + "]";
  }

  @Override
  public int hashCode() {
    if (len == 0) return 0;

    int result = 1;
    for (int i = 0; i < len; ++i) {
      result = 31 * result + (buf[off + i] & 0xff);
    }
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof BytesSlice)) return false;

    if (obj instanceof final ByteArraySlice other) {
      return BytesUtil.equals(buf, off, len, other.buf, other.off, other.len);
    }

    return slowCompare(this, (BytesSlice)obj) == 0;
  }

  @Override
  public int compareTo(final BytesSlice obj) {
    if (obj instanceof final ByteArraySlice other) {
      return BytesUtil.compare(buf, off, len, other.buf, other.off, other.len);
    }
    return slowCompare(this, obj);
  }

  public static int slowCompare(final BytesSlice a, final BytesSlice b) {
    final int len = Math.min(a.length(), b.length());
    for (int i = 0; i < len; ++i) {
      final int cmp = Integer.compare(a.get(i), b.get(i));
      if (cmp != 0) return cmp;
    }
    return a.length() - b.length();
  }
}
