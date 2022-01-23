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

import tech.dnaco.util.BitUtil;

public class BitDecoder {
  private final byte[] buffer;
  private int offset;

  private long vBuffer = 0;
  private int vBitsAvail = 0;

  public BitDecoder(final byte[] buffer) {
    this(buffer, 0);
  }

  public BitDecoder(final byte[] buffer, final int offset) {
    this.buffer = buffer;
    this.offset = offset;
  }

  public long readSigned(final int bits) {
    final long value = read(bits);
    final long signMask = (1L << (bits - 1));
    return ((value & signMask) == 0) ? value : -(value & (signMask - 1));
  }

  public long read(int bits) {
    if (bits > 64) {
      throw new IllegalArgumentException("Expected 64bit max, got " + bits + "bits");
    }

    long value = 0;
    while (bits > 0) {
      if (vBitsAvail == 0) {
        vBuffer = buffer[offset++] & 0xff;
        vBitsAvail = Byte.SIZE;
      }

      if (bits <= vBitsAvail) {
        final int shift = vBitsAvail - bits;
        final long mask = BitUtil.mask(bits);
        value |= (vBuffer >> shift) & mask;
        vBitsAvail -= bits;
        break;
      }

      value |= (vBuffer & BitUtil.mask(vBitsAvail)) << (bits - vBitsAvail);
      bits -= vBitsAvail;
      vBitsAvail = 0;
    }
    return value;
  }

  public int readAsInt(final int bits) {
    if (bits > 32) {
      throw new IllegalArgumentException("Expected 32bit max, got " + bits + "bits");
    }
    return Math.toIntExact(read(bits));
  }

  public boolean readBit() {
    return read(1) == 1;
  }

  public int nextClearBit(final int maxBits) {
    int value = 0;
    for (int i = 0; i < maxBits; ++i) {
      value <<= 1;
      if (readBit()) {
        value |= 1;
      } else {
        break;
      }
    }
    return value;
  }
}
