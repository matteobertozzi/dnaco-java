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

package tech.dnaco.bytes.encoding;

import tech.dnaco.bytes.ByteArrayAppender;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.LongValue;

public final class VarInt {
  private VarInt() {
    // no-op
  }

  private static long shift(final byte v, final int shift) {
    return ((long) (v & 0xff)) << shift;
  }

  public static int read(final ByteArraySlice slice, final LongValue result) {
    return read(slice, 0, result);
  }

  public static int read(final ByteArraySlice slice, final int offset, final LongValue result) {
    return read(slice.rawBuffer(), slice.offset() + offset, slice.length(), result);
  }

  public static int read(final ByteArraySlice slice, final int offset, final int length, final LongValue result) {
    return read(slice.rawBuffer(), slice.offset() + offset, length, result);
  }

  public static int read(final byte[] z, final int n, final LongValue result) {
    return read(z, 0, n, result);
  }

  public static int read(final byte[] buffer, final int off, final int n, final LongValue result) {
    if (n < 1) return 0;

    final int z0 = buffer[off] & 0xff;
    if (z0 <= 240) {
      result.set(z0);
      return 1;
    }
    if (z0 <= 248) {
      if (n < 2) return 0;
      result.set((z0 - 241) * 256 + shift(buffer[off+1], 0) + 240);
      return 2;
    }
    if (n < z0 - 246) return 0;
    if (z0 == 249) {
      result.set(2288 + 256 * shift(buffer[off+1], 0) + shift(buffer[off+2], 0));
      return 3;
    }
    if (z0 == 250) {
      result.set(shift(buffer[off+1], 16) + shift(buffer[off+2], 8) + shift(buffer[off+3], 0));
      return 4;
    }

    final long x = shift(buffer[off+1], 24) + shift(buffer[off+2], 16) + shift(buffer[off+3], 8) + (buffer[off+4] & 0xff);
    if (z0 == 251) {
      result.set(x);
      return 5;
    }
    if (z0 == 252) {
      result.set((x << 8) + (buffer[off+5] & 0xff));
      return 6;
    }
    if (z0 == 253) {
      result.set((x << 16) + shift(buffer[off+5], 8) + shift(buffer[off+6], 0));
      return 7;
    }
    if (z0 == 254) {
      result.set((x << 24) + shift(buffer[off+5], 16) + shift(buffer[off+6], 8) + shift(buffer[off+7], 0));
      return 8;
    }
    result.set((x << 32) + shift(buffer[off+5], 24) + (shift(buffer[off+6], 16) + (shift(buffer[off+7], 8) + shift(buffer[off+8], 0))));
    return 9;
  }

  /*
   ** Write a 32-bit unsigned integer as 4 big-endian bytes.
   */
  private static void write32(final byte[] buffer, final int off, final long y) {
    buffer[off] = (byte) ((y >> 24) & 0xff);
    buffer[off + 1] = (byte) ((y >> 16) & 0xff);
    buffer[off + 2] = (byte) ((y >> 8) & 0xff);
    buffer[off + 3] = (byte) (y & 0xff);
  }

  private static void write32(final ByteArrayAppender buf, final long y) {
    buf.add((int) ((y >> 24) & 0xff));
    buf.add((int) ((y >> 16) & 0xff));
    buf.add((int) ((y >> 8) & 0xff));
    buf.add((int) (y & 0xff));
  }

  public static int write(final ByteArrayAppender buf, final long x) {
    if (x < 0) {
      buf.add(255);
      write32(buf, x >> 32);
      write32(buf, x);
      return 9;
    }

    if (x <= 240) {
      buf.add((int) (x & 0xff));
      return 1;
    }

    final long y;
    if (x <= 2287) {
      y = (x - 240);
      buf.add((int) (y / 256 + 241));
      buf.add((int) (y % 256));
      return 2;
    }
    if (x <= 67823) {
      y = (x - 2288);
      buf.add(249);
      buf.add((int) (y / 256));
      buf.add((int) (y % 256));
      return 3;
    }
    y = x;
    final long w = (x >> 32);
    if (w == 0) {
      if (y <= 16777215) {
        buf.add(250);
        buf.add((int) (y >> 16));
        buf.add((int) (y >> 8));
        buf.add((int) y);
        return 4;
      }
      buf.add(251);
      buf.add((int) y);
      return 5;
    }
    if (w <= 255) {
      buf.add(252);
      buf.add((int) w);
      write32(buf, y);
      return 6;
    }
    if (w <= 65535) {
      buf.add(253);
      buf.add((int) (w >> 8));
      buf.add((int) w);
      write32(buf, y);
      return 7;
    }
    if (w <= 16777215) {
      buf.add(254);
      buf.add((int) (w >> 16));
      buf.add((int) (w >> 8));
      buf.add((int) w);
      write32(buf, y);
      return 8;
    }
    buf.add(255);
    write32(buf, w);
    write32(buf, y);
    return 9;
  }

  /*
   ** Write a varint into z[]. The buffer z[] must be at least 9 characters long to
   * accommodate the largest possible varint. Return the number of bytes of z[]
   * used.
   */
  public static int write(final byte[] buffer, final long value) {
    if (value < 0) {
      buffer[0] = (byte) 255;
      write32(buffer, 1, value >> 32);
      write32(buffer, 5, value);
      return 9;
    }

    if (value <= 240) {
      buffer[0] = (byte) (value & 0xff);
      return 1;
    }

    final long y;
    if (value <= 2287) {
      y = (value - 240);
      buffer[0] = (byte) (y / 256 + 241);
      buffer[1] = (byte) (y % 256);
      return 2;
    }
    if (value <= 67823) {
      y = (value - 2288);
      buffer[0] = (byte) 249;
      buffer[1] = (byte) (y / 256);
      buffer[2] = (byte) (y % 256);
      return 3;
    }
    y = value;
    final long w = (value >> 32);
    if (w == 0) {
      if (y <= 16777215) {
        buffer[0] = (byte) 250;
        buffer[1] = (byte) (y >> 16);
        buffer[2] = (byte) (y >> 8);
        buffer[3] = (byte) (y);
        return 4;
      }
      buffer[0] = (byte) 251;
      write32(buffer, 1, y);
      return 5;
    }
    if (w <= 255) {
      buffer[0] = (byte) 252;
      buffer[1] = (byte) w;
      write32(buffer, 2, y);
      return 6;
    }
    if (w <= 65535) {
      buffer[0] = (byte) 253;
      buffer[1] = (byte) (w >> 8);
      buffer[2] = (byte) w;
      write32(buffer, 3, y);
      return 7;
    }
    if (w <= 16777215) {
      buffer[0] = (byte) 254;
      buffer[1] = (byte) (w >> 16);
      buffer[2] = (byte) (w >> 8);
      buffer[3] = (byte) w;
      write32(buffer, 4, y);
      return 8;
    }
    buffer[0] = (byte) 255;
    write32(buffer, 1, w);
    write32(buffer, 5, y);
    return 9;
  }

  /*
   ** Return the number of bytes required to encode value v as a varint.
   */
  public static int length(final long value) {
    if (value < 0) {
      throw new IllegalArgumentException("expected value >= 0, got " + value);
    }

    if (value <= 240L) return 1;
    else if (value <= 2287L) return 2;
    else if (value <= 67823L) return 3;
    else if (value <= ((1L << 24) - 1)) return 4;
    else if (value <= ((1L << 32) - 1)) return 5;
    else if (value <= ((1L << 40) - 1)) return 6;
    else if (value <= ((1L << 48) - 1)) return 7;
    else if (value <= ((1L << 56) - 1)) return 8;
    return 9;
  }

  public static int bitCount(final long value) {
    return length(value) * 8;
  }
}
