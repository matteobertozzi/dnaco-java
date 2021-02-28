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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import tech.dnaco.collections.arrays.ArrayUtil;

public class ConcurrentHistogram implements TelemetryCollector {
  private final long[] bounds;
  private final AtomicLongArray events;
  private final AtomicLong maxValue = new AtomicLong(0);

  public ConcurrentHistogram(final long... bounds) {
    if (ArrayUtil.isEmpty(bounds)) {
      throw new UnsupportedOperationException("expected a list of bounds");
    }
    this.bounds = bounds;
    this.events = new AtomicLongArray(bounds.length + 1);
    clear();
  }

  public void clear() {
    for (int i = 0; i < events.length(); ++i) {
      events.set(i, 0);
    }
    this.maxValue.set(Math.min(0, bounds[0]));
  }

  public void add(final long value) {
    add(value, 1);
  }

  public void add(final long value, final long numEvents) {
    int index = 0;
    while ((index < bounds.length) && (value > bounds[index])) {
      index++;
    }
    events.addAndGet(index, numEvents);
    while (true) {
      final long cmax = maxValue.get();
      if (cmax >= value || maxValue.compareAndSet(cmax, value)) {
        break;
      }
    }
  }

	@Override
	public String getType() {
		return "HISTOGRAM";
	}

	@Override
	public TelemetryCollectorData getSnapshot() {
    final int nBounds = bounds.length;
    int index = 0;
    long nEvents = 0;
    final long maxValue = this.maxValue.get();
    while ((index < nBounds) && (maxValue > bounds[index])) {
      nEvents += events.get(index);
      index++;
    }
    nEvents += events.get(index);

    if (nEvents == 0) return HistogramData.EMPTY;

    final long[] snapshotBounds = new long[index + 1];
    final long[] snapshotEvents = new long[index + 1];
    System.arraycopy(bounds, 0, snapshotBounds, 0, index);
    snapshotBounds[index] = maxValue;
    for (int i = 0; i < snapshotEvents.length; ++i) {
      snapshotEvents[i] = events.get(i);
    }
    return new HistogramData(snapshotBounds, snapshotEvents);
	}
}
