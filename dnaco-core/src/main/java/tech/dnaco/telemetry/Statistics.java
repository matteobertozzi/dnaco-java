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

public final class Statistics {
  private Statistics() {
    // no-op
  }

  public static class StatisticsException extends Exception {
    private static final long serialVersionUID = -8844467628736709545L;

    public StatisticsException(final String msg) {
      super(msg);
    }
  }

  // ================================================================================
  //  Mean Related
  // ================================================================================
  public static float mean(final long... data) throws StatisticsException {
    if (data.length < 1) {
      throw new StatisticsException("mean requires at least one data point");
    }
    return (float) xsum(data) / data.length;
  }

  public static float mean(final long[] bounds, final long[] events) {
    long xsum = 0;
    long numEvents = 0;
    for (int i = 0; i < bounds.length; ++i) {
      xsum += bounds[i] * events[i];
      numEvents += events[i];
    }
    return Math.min(bounds[bounds.length - 1], (float) xsum / numEvents);
  }

  // ================================================================================
  //  Percentile Related
  // ================================================================================
  public static float percentile(final float p, final long[] bounds, final long[] events, final long maxValue, final long nevents) {
    if (nevents == 0) return 0;

    final float threshold = nevents * (p * 0.01f);
    float xsum = 0;
    for (int i = 0; (i < bounds.length) && (maxValue > bounds[i]); ++i) {
      xsum += events[i];
      if (xsum >= threshold) {
        // Scale linearly within this bucket
        final float left_point = bounds[(i == 0) ? 0 : (i - 1)];
        final float right_point = bounds[i];
        final float left_xsum = xsum - events[i];
        final float right_xsum = xsum;
        float pos = 0;
        final float right_left_diff = right_xsum - left_xsum;
        if (right_left_diff != 0) {
          pos = (threshold - left_xsum) / right_left_diff;
        }
        final float r = left_point + ((right_point - left_point) * pos);
        return (r > maxValue) ? maxValue : r;
      }
    }
    return maxValue;
  }

  // ================================================================================
  //  Others...
  // ================================================================================
  public static long xsum(final long... data) {
    return xsum(data, 0, data.length);
  }

  public static long xsum(final long[] data, final int offset, final int length) {
    long xsum = 0;
    for (int i = offset; i < length; ++i) {
      xsum += data[i];
    }
    return xsum;
  }

  public static long max(final long... data) {
    long xmax = Long.MIN_VALUE;
    for (int i = 0; i < data.length; ++i) {
      xmax = Math.max(xmax, data[i]);
    }
    return xmax;
  }

  // ================================================================================
  //  Rate related
  // ================================================================================
  public static float rateSec(final long[] counters, final int interval, final TimeUnit intervalUnit) {
    return xsum(counters, counters.length - interval, counters.length) / (interval * intervalUnit.toSeconds(1));
  }

  public static float rateMin(final long[] counters, final int interval, final TimeUnit intervalUnit) {
    return xsum(counters, counters.length - interval, counters.length) / (interval * intervalUnit.toMinutes(1));
  }
}
