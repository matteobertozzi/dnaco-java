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

public class ConcurrentMaxAndAvgTimeRangeGauge extends MaxAndAvgTimeRangeGauge {
  public ConcurrentMaxAndAvgTimeRangeGauge(final long maxInterval, final long window, final TimeUnit unit) {
    super(maxInterval, window, unit);
  }

  public void clear(final long now) {
    synchronized (this) {
      super.clear(now);
    }
  }

  public void update(final long value) {
    set(TimeUtil.currentUtcMillis(), value);
  }

  public void set(final long now, final long value) {
    synchronized (this) {
      super.set(TimeUtil.currentUtcMillis(), value);
    }
  }

  @Override
  public MaxAndAvgTimeRangeGaugeData getSnapshot() {
    synchronized (this) {
      return super.getSnapshot();
    }
  }
}
