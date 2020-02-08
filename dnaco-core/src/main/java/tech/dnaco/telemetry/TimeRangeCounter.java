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
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TimeRangeCounter implements TelemetryCollector {
  private final long[] counters;
  private final long window;

  private long lastInterval = Long.MAX_VALUE;
  private long next;

  public TimeRangeCounter(final long maxInterval, final long window, final TimeUnit unit) {
    this.window = unit.toMillis(window);

    final int trcCount = (int) Math.ceil(unit.toMillis(maxInterval) / (float) this.window);
    this.counters = new long[trcCount];
    this.clear(System.currentTimeMillis());
  }

  public void clear(final long now) {
    for (int i = 0; i < counters.length; ++i) {
      this.counters[i] = 0;
    }
    setLastInterval(now);
    this.next = 0;
  }

  public long inc() {
    return add(System.currentTimeMillis(), 1);
  }

  public long dec() {
    return add(System.currentTimeMillis(), -1);
  }

  public long inc(final long amount) {
    return add(System.currentTimeMillis(), amount);
  }

  public long add(final long now, final long delta) {
    if ((now - lastInterval) < window) {
      final int index = Math.toIntExact(next % counters.length);
      counters[index] += delta;
      return counters[index];
    }

    final int index;
    if ((now - lastInterval) >= window) {
      injectZeros(now);
      index = Math.toIntExact(++next % counters.length);
      setLastInterval(now);
      counters[index] = computeNewValue(delta);
    } else {
      index = Math.toIntExact(next % counters.length);
      counters[index] += delta;
    }
    return counters[index];
  }

  @Override
  public String getType() {
    return "TIME_RANGE_COUNTER";
  }

  @Override
  public TimeRangeCounterData getSnapshot() {
    final long[] data = new long[(int) Math.min(next + 1, counters.length)];
    for (int i = 0, n = data.length; i < n; ++i) {
      data[data.length - (i + 1)] = counters[Math.toIntExact((next - i) % counters.length)];
    }
    System.out.println(" -> " + Arrays.toString(data));
    return new TimeRangeCounterData(lastInterval, window, data);
  }

  protected long computeNewValue(final long newValue) {
    return newValue;
  }

  protected long getPrevValue() {
    return counters[Math.toIntExact((next - 1) % counters.length)];
  }

  private void setLastInterval(final long now) {
    this.lastInterval = (now - (now % window));
  }

  protected void injectZeros(final long now) {
    injectZeros(now, false);
  }

  protected void injectZeros(final long now, final boolean keepPrev) {
    if ((now - lastInterval) < window) return;

    final long slots = Math.round((now - lastInterval) / (float) window) - 1;
    if (slots > 0) {
      final long value = keepPrev ? counters[(int) (next % counters.length)] : 0;
      for (long i = 0; i < slots; ++i) {
        final long index = ++this.next;
        counters[(int) (index % counters.length)] = value;
      }
      setLastInterval(now);
    }
  }

  public static void main(final String[] args) {
    final TimeRangeCounter trc = new TimeRangeCounter(10, 1, TimeUnit.SECONDS);
    final long now = trc.lastInterval;
    System.out.println(new Date(trc.lastInterval));
    System.out.println(Arrays.toString(trc.counters));
    for (int i = 0; i < 20; ++i) {
      trc.add(now + (1000 * i), 1 + i);
    }
    System.out.println(new Date(trc.lastInterval));
    System.out.println(Arrays.toString(trc.counters));
    trc.getSnapshot();
  }
}
