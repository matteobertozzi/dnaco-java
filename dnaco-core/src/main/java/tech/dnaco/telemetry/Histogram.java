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

package tech.dnaco.telemetry;

import java.util.Arrays;

import tech.dnaco.collections.arrays.ArrayUtil;

public class Histogram implements TelemetryCollector {
  public static final long[] DEFAULT_DURATION_BOUNDS_MS = new long[] {
    5, 10, 25, 50, 75, 100, 150, 250, 350, 500, 750,  // msec
    1000, 2500, 5000, 10000, 25000, 50000, 60000,     // sec
    75000, 120000,                                    // min
  };

  public static final long[] DEFAULT_SMALL_DURATION_BOUNDS_NS = new long[] {
    25, 50, 75, 100, 500, 1_000,                                      // nsec
    10_000, 25_000, 50_000, 75_000, 100_000,                          // usec
    250_000, 500_000, 750_000,
    1_000_000, 5000000L, 10000000L, 25000000L, 50000000L, 75000000L,  // msec
  };

  public static final long[] DEFAULT_DURATION_BOUNDS_NS = new long[] {
    25, 50, 100,                                                              // nsec
    1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 750_000,                // usec
    1_000_000, 5000000L, 10000000L, 25000000L, 50000000L, 75000000L,          // msec
    100000000L, 150000000L, 250000000L, 350000000L, 500000000L, 750000000L,   // msec
    1000000000L, 2500000000L, 5000000000L, 10000000000L, 25000000000L,        // sec
    60000000000L, 120000000000L, 300000000000L, 600000000000L, 900000000000L  // min
  };

  public static final long[] DEFAULT_SIZE_BOUNDS = new long[] {
    0, 128, 256, 512,
    1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10, // kb
    1 << 20, 2 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20, 128 << 20, 256 << 20, 512 << 20, // mb
  };

  public static final long[] DEFAULT_SMALL_SIZE_BOUNDS = new long[] {
    0, 32, 64, 128, 256, 512,
    1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10,
    64 << 10, 128 << 10, 256 << 10, 512 << 10, // kb
    1 << 20
  };

  public static final long[] DEFAULT_COUNT_BOUNDS = new long[] {
    0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000,
    2000, 2500, 5000, 10_000, 15_000, 20_000, 25_000,
    50_000, 75_000, 100_000, 250_000, 500_000, 1_000_000
  };

  // 10, 20, 30
  //  1, 0,  0
  // maxValue = 5
  // bounds = [5], events = [1]
  // 10, 20, 30
  //  1, 1,  0
  // maxValue = 18
  // bounds = [10, 18], events = [1, 1]

  // 1: bounds=[5] envets=[1]
  // 2: bounds=[10, 18] events=[1,1]
  // 3: bounds=[10, 20, 25] events=[1,1]
  // ---> [10, 20, 25]
  private final long[] bounds;
  private final long[] events;
  private long maxValue;

  public Histogram(final long[] bounds) {
    if (ArrayUtil.isEmpty(bounds)) {
      throw new UnsupportedOperationException("expected a list of bounds");
    }

    this.bounds = bounds;
    this.events = new long[bounds.length + 1];
    this.clear();
  }

  public void clear() {
    Arrays.fill(events, 0);
    maxValue = Math.min(0, bounds[0]);
  }

  public void add(final long value) {
    add(value, 1);
  }

  public void add(final long value, final long nEvents) {
    final int nBounds = bounds.length;
    int index = 0;
    while ((index < nBounds) && (value > bounds[index])) {
      index++;
    }

    events[index] += nEvents;
    maxValue = Math.max(maxValue, value);
  }

  @Override
  public String getType() {
    return "HISTOGRAM";
  }

  @Override
  public HistogramData getSnapshot() {
    final int nBounds = bounds.length;
    int index = 0;
    long nEvents = 0;
    while ((index < nBounds) && (maxValue > bounds[index])) {
      nEvents += events[index];
      index++;
    }
    nEvents += events[index];

    if (nEvents == 0) return HistogramData.EMPTY;

    final long[] snapshotBounds = new long[index + 1];
    final long[] snapshotEvents = new long[index + 1];
    System.arraycopy(bounds, 0, snapshotBounds, 0, index);
    snapshotBounds[index] = maxValue;
    System.arraycopy(events, 0, snapshotEvents, 0, index + 1);
    return new HistogramData(snapshotBounds, snapshotEvents);
  }
}
