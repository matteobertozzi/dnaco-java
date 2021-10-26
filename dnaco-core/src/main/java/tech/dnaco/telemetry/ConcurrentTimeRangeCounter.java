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

public class ConcurrentTimeRangeCounter extends TimeRangeCounter {
  public ConcurrentTimeRangeCounter(final long maxInterval, final long window, final TimeUnit unit) {
    super(maxInterval, window, unit);
  }

  @Override
  public void clear(final long now) {
    synchronized (this) {
      super.clear(now);
    }
  }

  @Override
  public long inc() {
    return add(TimeUtil.currentUtcMillis(), 1);
  }

  @Override
  public long dec() {
    return add(TimeUtil.currentUtcMillis(), -1);
  }

  @Override
  public long inc(final long amount) {
    return add(TimeUtil.currentUtcMillis(), amount);
  }

  @Override
  public long add(final long now, final long delta) {
    synchronized (this) {
      return super.add(now, delta);
    }
  }

  @Override
  public void update(final long value) {
    update(TimeUtil.currentUtcMillis(), value);
  }

  @Override
  public void update(final long now, final long value) {
    synchronized (this) {
      super.update(now, value);
    }
  }

  @Override
  public String getType() {
    return "TIME_RANGE_COUNTER";
  }

  @Override
  public TimeRangeCounterData getSnapshot() {
    synchronized (this) {
      return super.getSnapshot();
    }
  }
}
