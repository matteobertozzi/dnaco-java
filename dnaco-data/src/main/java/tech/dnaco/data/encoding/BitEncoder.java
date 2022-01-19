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

package tech.dnaco.data.encoding;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.util.BitUtil;

public class BitEncoder {
  private final PagedByteArray buffer;

  private long vBuffer = 0;
  private int vBitsAvail = Long.SIZE;

  public BitEncoder(final PagedByteArray buffer) {
    this.buffer = buffer;
  }

  public BitEncoder(final int pageSize) {
    this.buffer = new PagedByteArray(pageSize);
  }

  public PagedByteArray buffer() {
    return buffer;
  }

  public void add(final boolean value) {
    add(value ? 1 : 0, 1);
  }

  public void addZero() {
    add(0, 1);
  }

  public void addOne() {
    add(1, 1);
  }

  public void addSigned(final long value, final int bits) {
    if (value >= 0) {
      verify(value, bits - 1);
      add(value, bits);
    } else {
      final long signedValue = ((-value) & BitUtil.mask(bits - 1)) | (1L << (bits - 1));
      add(signedValue, bits);
    }
  }

  private static void verify(final long value, final int bits) {
    if (value != (value & BitUtil.mask(bits))) {
      throw new IllegalArgumentException("value " + value + " does not fit " + bits + "bits");
    }
  }

  public void add(final long value, final int bits) {
    verify(value, bits);

    if (bits <= vBitsAvail) {
      vBitsAvail -= bits;
      vBuffer |= (value << vBitsAvail);
      if (vBitsAvail == 0) flush(Long.SIZE);
      return;
    }

    // write the first part and flush
    final int shift = bits - vBitsAvail;
    final int remainingBits = shift;
    vBuffer |= (value >> shift) & BitUtil.mask(vBitsAvail);
    flush(Long.SIZE);

    // write the second part
    vBitsAvail -= remainingBits;
    vBuffer |= (value & BitUtil.mask(remainingBits)) << vBitsAvail;
  }

  public void flush() {
    flush(Long.SIZE - vBitsAvail);
  }

  private void flush(final int bits) {
    for (int i = 1, n = (bits + 7) >> 3; i <= n; ++i) {
      buffer.add((int)((vBuffer >> (Long.SIZE - (i << 3))) & 0xff));
    }
    vBuffer = 0;
    vBitsAvail = Long.SIZE;
  }

  public static void main(final String[] args) {
    for (int i = 0; i < 63; ++i) {
      final byte[] buf = encode(1L << (63 - i), i + 1);
      if (decode(buf, i + 1) != (1L << (63 - i))) {
        throw new IllegalArgumentException();
      }
    }
  }

  private static byte[] encode(final long vBuffer, final int bits) {
    final byte[] buf = new byte[8];
    System.out.println(((bits + 7) >> 3) + " -> " + Long.toBinaryString(vBuffer));
    for (int i = 1, n = (bits + 7) >> 3; i <= n; ++i) {
      buf[i - 1] = (byte)((vBuffer >> (Long.SIZE - (i << 3))) & 0xff);
      System.out.println("WRITE[" + i + "] -> " + (Long.SIZE - (i << 3)) + " = " + ((vBuffer >> (Long.SIZE - (i << 3))) & 0xff));
    }
    return buf;
  }

  private static long decode(final byte[] buf, final int bits) {
    long value = 0;
    for (int i = 1, n = (bits + 7) >> 3; i <= n; ++i) {
      System.out.println("READ[" + i + "] -> " + (Long.SIZE - (i << 3)) + " = " + ((buf[i-1] & 0xff)));
      value |= ((long)(buf[i - 1] & 0xff)) << (Long.SIZE - (i << 3));
    }
    System.out.println(Long.toBinaryString(value));
    return value;
  }
}
