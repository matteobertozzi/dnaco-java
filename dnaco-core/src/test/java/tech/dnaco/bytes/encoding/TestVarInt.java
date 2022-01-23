/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.bytes.encoding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.Test;

import tech.dnaco.collections.LongValue;

public class TestVarInt {
  private static class TestRand {
    private long rx = 1;
    private long ry = 0;

    long nextLong() {
      rx = (rx >> 1) ^ (-(rx & 1) & 0xd0000001);
      ry = ry * 1103515245 + 12345;
      return rx ^ ry;
    }
  }

  @Test
  public void testVarInt() {
    final Random rand = new Random();
    testEncodeDecode(100_000_000, rand::nextLong);
  }

  @Test
  public void testVarInt2() {
    final TestRand rand = new TestRand();
    testEncodeDecode(100_000_000, rand::nextLong);
  }

  private static void testEncodeDecode(final int count, final LongSupplier rand) {
    final LongValue y = new LongValue();
    long px = 0;
    int pn = 0;
    final byte[] z = new byte[20];
    final byte[] zp = new byte[20];

    for (int i = 0; i < 100_000_000; i++) {
      final long x = rand.getAsLong();

      final int n1 = VarInt.write(z, x);
      assertTrue(n1 >= 1 && n1 <= 9);
      int n2 = VarInt.read(z, n1, y);
      if (n1 != n2) {
        throw new IllegalArgumentException(i + " n1=" + n1 + " n2=" + n2);
      }
      if (x != y.get()) {
        throw new IllegalArgumentException(i + " x=" + x + " y=" + y.get() + " n=" + n1 + " " + toUnsignedArray(z));
      }
      n2 = VarInt.read(z, n1 - 1, y);
      assertEquals(0, n2);
      if (i > 0) {
        final int c = Arrays.compareUnsigned(z, 0, Math.min(pn, n1), zp, 0, Math.min(pn, n1));
        if ((x >= 0 && px >= 0 && x < px) || (px < 0 && x >= 0) || (x < 0 && px < 0 && x <= px)) {
          if (!(c < 0)) {
            throw new IllegalArgumentException(i + " non memcmparable x=" + x + " px=" + px + " pn=" + pn + " c=" + c
                + " z=" + toUnsignedArray(z) + " zp" + toUnsignedArray(zp));
          }
        } else if ((x >= 0 && px >= 0 && x > px) || (x < 0 && px >= 0) || (x < 0 && px < 0 && x >= px)) {
          if (!(c > 0)) {
            throw new IllegalArgumentException(i + " non memcmparable x=" + x + " px=" + px + " pn=" + pn + " c=" + c
                + " z=" + toUnsignedArray(z) + " zp" + toUnsignedArray(zp));
          }
        } else {
          if (c != 0) {
            throw new IllegalArgumentException(i + " non memcmparable x=" + x + " px=" + px + " pn=" + pn + " c=" + c
                + " z=" + toUnsignedArray(z) + " zp" + toUnsignedArray(zp));
          }
        }
      }
      System.arraycopy(z, 0, zp, 0, n1);
      pn = n1;
      px = x;
    }

  }

  static String toUnsignedArray(final byte[] v) {
    final StringBuilder builder = new StringBuilder();
    builder.append("[");
    for (int i = 0; i < v.length; ++i) {
      if (i > 0)
        builder.append(", ");
      builder.append(v[i] & 0xff);
    }
    builder.append("]");
    return builder.toString();
  }
}
