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

package tech.dnaco.util;

public final class BitUtil {
  private BitUtil() {
    // no-op
  }

  public static boolean isBitSet(final int value, final int bit) {
    final int bitVal = 1 << (bit - 1);
    return (value & bitVal) == bitVal;
  }

  public static int align(final int value, final int alignment) {
    return (value + (alignment - 1)) & -alignment;
  }

  public static long align(final long value, final int alignment) {
    return (value + (alignment - 1)) & -alignment;
  }

  public static boolean isPow2(final int value) {
    return value > 0 && ((value & (~value + 1)) == value);
  }

  public static boolean isPow2(final long value) {
    return value > 0 && ((value & (~value + 1)) == value);
  }

  public static int nextPow2(final int value) {
    return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
  }

  public static long nextPow2(final long value) {
    return 1L << (Long.SIZE - Long.numberOfLeadingZeros(value - 1));
  }

  public static long mask(final int bits) {
    return (bits == 64) ? 0xffffffffffffffffL : ((1L << bits) - 1);
  }
}
