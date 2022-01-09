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

public class IntXorEncoder {
  private final BitEncoder bitWriter;

  private int lastLeadingZeros = Integer.MAX_VALUE;
  private int lastTrailingZeros = 0;
  private long lastValue = 0;

  public IntXorEncoder(final BitEncoder bitWriter) {
    this.bitWriter = bitWriter;
  }

  public void addFirst(final double value) {
    addFirst(Double.doubleToRawLongBits(value));
  }

  public void addFirst(final long value) {
    bitWriter.add(value, 64);
    this.lastValue = value;
  }

  public void add(final double value) {
    add(Double.doubleToRawLongBits(value));
  }

  public void add(final long value) {
    final long xor = lastValue ^ value;
    if (xor == 0) {
      bitWriter.addZero();
    } else {
      int leadingZeros = Long.numberOfLeadingZeros(xor);
      final int trailingZeros = Long.numberOfTrailingZeros(xor);

      // Check overflow of leading? Can't be 32!
      if (leadingZeros >= 32) {
        leadingZeros = 31;
      }

      // Store bit '1'
      bitWriter.addOne();

      if (leadingZeros >= lastLeadingZeros && trailingZeros >= lastTrailingZeros) {
        writeExistingLeading(xor);
      } else {
        writeNewLeading(xor, leadingZeros, trailingZeros);
      }
    }

    lastValue = value;
  }

  private void writeExistingLeading(final long xor) {
    bitWriter.addZero();

    final int significantBits = 64 - lastLeadingZeros - lastTrailingZeros;
    bitWriter.add(xor >>> lastTrailingZeros, significantBits);
  }

  private void writeNewLeading(final long xor, final int leadingZeros, final int trailingZeros) {
    bitWriter.addOne();

    // Different from version 1.x, use (significantBits - 1) in storage - avoids a branch
    final int significantBits = 64 - leadingZeros - trailingZeros;

    // Different from original, bits 5 -> 6, avoids a branch, allows storing small longs
    bitWriter.add(leadingZeros, 6); // Number of leading zeros in the next 6 bits
    bitWriter.add(significantBits - 1, 6); // Length of meaningful bits in the next 6 bits
    bitWriter.add(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

    lastLeadingZeros = leadingZeros;
    lastTrailingZeros = trailingZeros;
  }
}
