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

import java.util.concurrent.TimeUnit;

import tech.dnaco.time.TimeUtil;

public class MaxAndAvgTimeRangeGauge implements TelemetryCollector {
  private final long[] ringMax;
  private final long[] ringAvg;
  private final long window;

  private long lastInterval;
  private long next;
  private long count;
  private long sum;
  private long max;

  public MaxAndAvgTimeRangeGauge(final long maxInterval, final long window, final TimeUnit unit) {
    this.window = unit.toMillis(window);

    final int slots = (int) Math.ceil(unit.toMillis(maxInterval) / (float) this.window);
    this.ringMax = new long[slots];
    this.ringAvg = new long[slots];
    this.clear(TimeUtil.currentUtcMillis());
  }

  public void clear(final long now) {
    for (int i = 0, n = ringAvg.length; i < n; ++i) {
      ringAvg[i] = 0;
      ringMax[i] = 0;
    }
    this.setLastInterval(now);
    this.next = 0;
    this.count = 0;
    this.max = 0;
    this.sum = 0;
  }

  public void update(final long value) {
    set(TimeUtil.currentUtcMillis(), value);
  }

  public void set(final long now, final long value) {
    if ((now - lastInterval) >= window) {
      injectZeros(now);
      saveSnapshot();
      setLastInterval(now);
    }
    this.max = Math.max(max, value);
    this.sum += value;
    this.count++;
  }

  @Override
  public String getType() {
    return "MAX_AND_AVG_TIME_RANGE_GAUGE";
  }

  @Override
  public MaxAndAvgTimeRangeGaugeData getSnapshot() {
    saveSnapshot(Math.toIntExact(next % ringMax.length));

    final int slots = (int) Math.min(next + 1, ringMax.length);
    final long[] vMax = new long[slots];
    final long[] vAvg = new long[slots];
    for (int i = 0; i < slots; ++i) {
      final int ringIndex = Math.toIntExact((next - i) % ringMax.length);
      final int dataOffset = slots - (i + 1);
      vMax[dataOffset] = ringMax[ringIndex];
      vAvg[dataOffset] = ringAvg[ringIndex];
    }
    return new MaxAndAvgTimeRangeGaugeData(lastInterval, window, vAvg, vMax);
  }

  private void saveSnapshot() {
    saveSnapshot(Math.toIntExact(this.next++ % this.ringAvg.length));
  }

  private void saveSnapshot(final int index) {
    final long avg = this.count > 0 ? (this.sum / this.count) : 0;
    this.ringAvg[index] = avg;
    this.ringMax[index] = max;
    this.count = 1;
    this.sum = avg;
    this.max = avg;
  }

  protected void injectZeros(final long now) {
    if ((now - lastInterval) < window) return;

    final long slots = Math.round((now - lastInterval) / (float) window) - 1;
    for (long i = 0; i < slots; ++i) {
      saveSnapshot();
    }
  }

  private void setLastInterval(final long now) {
    this.lastInterval = now - (now % window);
  }
}
