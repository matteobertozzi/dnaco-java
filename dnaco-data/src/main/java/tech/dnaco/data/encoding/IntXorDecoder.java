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

public class IntXorDecoder {
  private final BitDecoder bitReader;

  private int lastLeadingZeros = Integer.MAX_VALUE;
  private int lastTrailingZeros = 0;
  private long lastValue = 0;

  public IntXorDecoder(final BitDecoder bitReader) {
    this.bitReader = bitReader;
  }

  public long readFirst() {
    final long value = bitReader.read(64);
    this.lastValue = value;
    return value;
  }

  public long readNext() {
    switch (bitReader.nextClearBit(2)) {
      case 3:
        // New leading and trailing zeros
        lastLeadingZeros = bitReader.readAsInt(6);
        final int significantBits = bitReader.readAsInt(6) + 1;

        lastTrailingZeros = Long.SIZE - significantBits - lastLeadingZeros;
        // missing break is intentional, we want to overflow to next one
      case 2:
        long value = bitReader.read(Long.SIZE - lastLeadingZeros - lastTrailingZeros);
        value <<= lastTrailingZeros;

        value = lastValue ^ value;
        lastValue = value;
        return value;
    }
    return lastValue;
  }
}
