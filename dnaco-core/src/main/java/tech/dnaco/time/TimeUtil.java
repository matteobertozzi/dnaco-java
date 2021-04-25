/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.time;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public final class TimeUtil {
  private static ClockProvider clockProvider = new SystemClockProvider();

  private TimeUtil() {
    // no-op
  }

  public static long currentUtcMillis() {
    return clockProvider.currentUtcMillis();
  }

  public static long currentNanos() {
    return clockProvider.currentNanos();
  }

  // ===========================================================================
  //  Clock Providers related
  // ===========================================================================
  public static void setClockProvider(final ClockProvider provider) {
    TimeUtil.clockProvider = provider;
  }

  public static ClockProvider getClockProvider() {
    return TimeUtil.clockProvider;
  }

  public interface ClockProvider {
    long currentUtcMillis();
    long epochMillis();
    long epochNanos();

    long currentNanos();
  }

  public static final class SystemClockProvider implements ClockProvider {
    @Override
    public long currentUtcMillis() {
      return System.currentTimeMillis();
    }

    @Override
    public long epochMillis() {
      return System.currentTimeMillis();
    }

    @Override
    public long epochNanos() {
      final Instant now = Instant.now();
      final long seconds = now.getEpochSecond();
      final long nanosFromSecond = now.getNano();
      return (seconds * 1_000_000_000L) + nanosFromSecond;
    }

    @Override
    public long currentNanos() {
      return System.nanoTime();
    }
  }

  public static class ManualClockProvider implements ClockProvider {
    private long nanos = 1;

    @Override
    public long currentUtcMillis() {
      return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    @Override
    public long epochMillis() {
      return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    @Override
    public long epochNanos() {
      return nanos;
    }

    @Override
    public long currentNanos() {
      return nanos;
    }

    public void setTime(final long value, final TimeUnit unit) {
      this.nanos = unit.toNanos(value);
    }

    public void incTime(final long value, final TimeUnit unit) {
      this.nanos += unit.toNanos(value);
    }

    public void decTime(final long value, final TimeUnit unit) {
      this.nanos -= unit.toNanos(value);
    }
  }
}