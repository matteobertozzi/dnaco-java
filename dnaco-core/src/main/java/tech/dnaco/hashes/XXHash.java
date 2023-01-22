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

package tech.dnaco.hashes;

import java.util.Arrays;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.util.RandData;

public final class XXHash {
  public static final long DEFAULT_SEED_64 = Math.round(RandData.random() * 0xdef3f6abd9L);
  public static final int DEFAULT_SEED_32 = Math.toIntExact(Math.round(RandData.random() * 0x19df3));

  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

  private static final int PRIME1 = 0x9E3779B1;
  private static final int PRIME2 = 0x85EBCA77;
  private static final int PRIME3 = 0xC2B2AE3D;
  private static final int PRIME4 = 0x27D4EB2F;
  private static final int PRIME5 = 0x165667B1;

  private XXHash() {
    // no-op
  }

  public static int hash32(final int seed, final byte[] input) {
    return hash32(seed, input, 0, BytesUtil.length(input));
  }

  public static int hash32(final int seed, final byte[] input, final int off, final int len) {
    final int end = off + len;
    int offset = off;
    int h32;
    if (len >= 16) {
      final int limit = end - 16;
      int v1 = seed + PRIME1 + PRIME2;
      int v2 = seed + PRIME2;
      int v3 = seed;
      int v4 = seed - PRIME1;
      do {
        v1 += getInt(input, offset) * PRIME2;
        v1 = Integer.rotateLeft(v1, 13) * PRIME1;
        offset += 4;
        v2 += getInt(input, offset) * PRIME2;
        v2 = Integer.rotateLeft(v2, 13) * PRIME1;
        offset += 4;
        v3 += getInt(input, offset) * PRIME2;
        v3 = Integer.rotateLeft(v3, 13) * PRIME1;
        offset += 4;
        v4 += getInt(input, offset) * PRIME2;
        v4 = Integer.rotateLeft(v4, 13) * PRIME1;
        offset += 4;
      } while (offset <= limit);
      h32 = Integer.rotateLeft(v1, 1)
          + Integer.rotateLeft(v2, 7)
          + Integer.rotateLeft(v3, 12)
          + Integer.rotateLeft(v4, 18);
    } else {
      h32 = seed + PRIME5;
    }

    for (h32 += len; offset <= end - 4; offset += 4) {
      h32 += getInt(input, offset) * PRIME3;
      h32 = Integer.rotateLeft(h32, 17) * PRIME4;
    }

    while (offset < end) {
      h32 += (input[offset] & 255) * PRIME5;
      h32 = Integer.rotateLeft(h32, 11) * PRIME1;
      ++offset;
    }

    h32 ^= h32 >>> 15;
    h32 *= PRIME2;
    h32 ^= h32 >>> 13;
    h32 *= PRIME3;
    h32 ^= h32 >>> 16;
    return h32;
  }

  public static long hash64(final long seed, final byte[] input) {
    return hash64(seed, input, 0, BytesUtil.length(input));
  }

  public static long hash64(final long seed, final byte[] input, final int off, final int len) {
    long hash;
    long remaining = len;
    int offset = off;

    if (remaining >= 32) {
      long v1 = seed + PRIME64_1 + PRIME64_2;
      long v2 = seed + PRIME64_2;
      long v3 = seed;
      long v4 = seed - PRIME64_1;

      do {
        v1 += getLong(input, offset) * PRIME64_2;
        v1 = Long.rotateLeft(v1, 31) * PRIME64_1;

        v2 += getLong(input, offset + 8) * PRIME64_2;
        v2 = Long.rotateLeft(v2, 31) * PRIME64_1;

        v3 += getLong(input, offset + 16) * PRIME64_2;
        v3 = Long.rotateLeft(v3, 31) * PRIME64_1;

        v4 += getLong(input, offset + 24) * PRIME64_2;
        v4 = Long.rotateLeft(v4, 31) * PRIME64_1;

        offset += 32;
        remaining -= 32;
      } while (remaining >= 32);

      hash = Long.rotateLeft(v1, 1)
           + Long.rotateLeft(v2, 7)
           + Long.rotateLeft(v3, 12)
           + Long.rotateLeft(v4, 18);

      v1 = Long.rotateLeft(v1 * PRIME64_2, 31) * PRIME64_1;
      hash = (hash ^ v1) * PRIME64_1 + PRIME64_4;

      v2 = Long.rotateLeft(v2 * PRIME64_2, 31) * PRIME64_1;
      hash = (hash ^ v2) * PRIME64_1 + PRIME64_4;

      v3 = Long.rotateLeft(v3 * PRIME64_2, 31) * PRIME64_1;
      hash = (hash ^ v3) * PRIME64_1 + PRIME64_4;

      v4 = Long.rotateLeft(v4 * PRIME64_2, 31) * PRIME64_1;
      hash = (hash ^ v4) * PRIME64_1 + PRIME64_4;
    } else {
      hash = seed + PRIME64_5;
    }

    hash += len;

    while (remaining >= 8) {
      long k1 = getLong(input, offset);
      k1 = Long.rotateLeft(k1 * PRIME64_2, 31) * PRIME64_1;
      hash = Long.rotateLeft(hash ^ k1, 27) * PRIME64_1 + PRIME64_4;
      offset += 8;
      remaining -= 8;
    }

    if (remaining >= 4) {
      hash ^= getInt(input, offset) * PRIME64_1;
      hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
      offset += 4;
      remaining -= 4;
    }

    while (remaining != 0) {
      hash ^= input[offset] * PRIME64_5;
      hash = Long.rotateLeft(hash, 11) * PRIME64_1;
      --remaining;
      ++offset;
    }

    hash ^= hash >>> 33;
    hash *= PRIME64_2;
    hash ^= hash >>> 29;
    hash *= PRIME64_3;
    hash ^= hash >>> 32;
    return hash;
  }

  private static int getInt(final byte[] bytes, final int pos) {
    return (bytes[pos] & 0xFF)
        | ((bytes[pos + 1] & 0xFF) << 8)
        | ((bytes[pos + 2] & 0xFF) << 16)
        | ((bytes[pos + 3] & 0xFF) << 24);
  }

  public static long getLong(final byte[] array, final int offset) {
    return (array[offset] & 0xFFL)
         | (array[offset + 1] & 0xFFL) << 8
         | (array[offset + 2] & 0xFFL) << 16
         | (array[offset + 3] & 0xFFL) << 24
         | (array[offset + 4] & 0xFFL) << 32
         | (array[offset + 5] & 0xFFL) << 40
         | (array[offset + 6] & 0xFFL) << 48
         | (array[offset + 7] & 0xFFL) << 56;
  }

  public static void main(final String[] args) {
    final int[] buckets32 = new int[16];
    final int[] buckets64 = new int[16];
    final byte[] key = new byte[128];

    for (int i = 0; i < 10_000; ++i) {
      RandData.generateBytes(key);
      final int hash32 = XXHash.hash32(0, key) & 0x7fffffff;
      buckets32[hash32 & (buckets32.length - 1)]++;
      final long hash64 = XXHash.hash64(0, key) & 0x7fffffffffffffffL;
      buckets64[Math.toIntExact(hash64 & (buckets64.length - 1))]++;
    }
    System.out.println(Arrays.toString(buckets32));
    System.out.println(Arrays.toString(buckets64));
  }
}
