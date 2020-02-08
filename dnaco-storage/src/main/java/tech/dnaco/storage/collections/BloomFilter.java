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

package tech.dnaco.storage.collections;

import java.util.BitSet;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.hash.XXHash;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.BitUtil;

public class BloomFilter {
  private final BitSet bitSet;
  private final int numHashFunctions;

  public BloomFilter(final long expectedInsertions, final double p) {
    final int numBits = Math.toIntExact(optimalNumOfBits(expectedInsertions, p));
    this.bitSet = new BitSet(numBits);
    this.numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
    System.out.println("NUM BITS " + HumansUtil.humanSize(numBits) + " NUM HASH FUNCTIONS " + numHashFunctions);
  }

  public BloomFilter(final long numBits, final int numHashFunctions) {
    this.bitSet = new BitSet(Math.toIntExact(BitUtil.align((numBits + 7) / 8, 64)));
    this.numHashFunctions = numHashFunctions;
  }

  public byte[] toByteArray() {
    return bitSet.toByteArray();
  }

  public void debug() {
    if (false) {
      for (int i = 0, n = bitSet.size(); i < n; ++i) {
        System.out.print(bitSet.get(i) ? 1 : 0);
        if ((i + 1) % 64 == 0) System.out.println();
      }
    }
    final long[] data = bitSet.toLongArray();
    for (int i = 0; i < data.length; ++i) {
      System.out.print(data[i] + " ");
      if ((i + 1) % 4 == 0) System.out.println();
    }
  }

  public boolean put(final ByteArraySlice key) {
    return put(key.rawBuffer(), key.offset(), key.length());
  }

  public boolean put(final byte[] buf, final int off, final int len) {
    final long hash64 = XXHash.hash64(XXHash.DEFAULT_SEED_64, buf, off, len);
    final int hashA = (int) hash64;
    final int hashB = (int) (hash64 >>> 32);

    final int numBits = bitSet.size();
    int collision = 0;
    for (int i = 1; i <= numHashFunctions; ++i) {
      int combinedHash = (hashA + (i * hashB));
      if (combinedHash < 0) combinedHash = ~combinedHash;
      final int index = combinedHash % numBits;
      collision += bitSet.get(index) ? 1 : 0;
      bitSet.set(index);
    }
    return (collision == numHashFunctions);
  }

  public boolean mightContain(final ByteArraySlice key) {
    return mightContain(key.rawBuffer(), key.offset(), key.length());
  }

  public boolean mightContain(final byte[] buf, final int off, final int len) {
    final long hash64 = XXHash.hash64(XXHash.DEFAULT_SEED_64, buf, off, len);
    final int hashA = (int) hash64;
    final int hashB = (int) (hash64 >>> 32);

    final int numBits = bitSet.size();
    for (int i = 1; i <= numHashFunctions; ++i) {
      int combinedHash = hashA + (i * hashB);
      if (combinedHash < 0) combinedHash = ~combinedHash;
      if (!bitSet.get(combinedHash % numBits)) {
        return false;
      }
    }
    return true;
  }

  public static int optimalNumOfHashFunctions(final long expectedInsertions, final long numBits) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) numBits / expectedInsertions * Math.log(2)));
  }

  public static long optimalNumOfBits(final long expectedInsertions, double p) {
    if (p == 0) p = Double.MIN_VALUE;
    return (long) (-expectedInsertions * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  public static void main(final String[] args) {
    final long n = optimalNumOfBits(2_182_360, 1 / 10_000_000.0);
    System.out.println(optimalNumOfHashFunctions(2_182_360, n));
    System.out.println(n);
    System.out.println((n + 7) / 8);
    System.out.println(HumansUtil.humanSize((n + 7) / 8));
    System.out.println(HumansUtil.humanSize(BitUtil.align(n / 8, 4 << 10)));
  }
}
