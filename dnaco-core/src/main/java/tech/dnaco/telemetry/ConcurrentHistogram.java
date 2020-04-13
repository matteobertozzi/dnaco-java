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

public class ConcurrentHistogram implements TelemetryCollector {
  private final long[] bounds;
  private final AtomicLongArray events;
  private final AtomicLong maxValue = new AtomicLong(0);

  public ConcurrentHistogram(final long... bounds) {
    assert bounds.length > 0;
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
    long numEvents = 0;
    final long[] snapshotEvents = new long[events.length()];
    for (int i = 0; i < snapshotEvents.length; ++i) {
      final long n = events.get(i);
      snapshotEvents[i] = n;
      numEvents += n;
    }

    if (numEvents == 0) {
      return HistogramData.EMPTY;
    }

    final long snapshotMaxValue = this.maxValue.get();
    final long[] snapshotBounds = new long[snapshotEvents.length];
    System.arraycopy(bounds, 0, snapshotBounds, 0, snapshotEvents.length - 1);
    snapshotBounds[snapshotEvents.length - 1] = snapshotMaxValue;
    return new HistogramData(snapshotBounds, snapshotEvents);
	}
}
