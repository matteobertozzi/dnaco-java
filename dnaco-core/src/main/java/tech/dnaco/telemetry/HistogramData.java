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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.JsonUtil;

public class HistogramData implements TelemetryCollectorData {
  public static final HistogramData EMPTY = new HistogramData(null, null);

  private final long[] bounds;
  private final long[] events;

  public HistogramData(final long[] bounds, final long[] events) {
    this.bounds = bounds;
    this.events = events;
  }

  public long[] getBounds() {
    return bounds;
  }

  public long[] getEvents() {
    return events;
  }

  public long getNumEvents() {
    return ArrayUtil.sum(events, 0, ArrayUtil.length(events));
  }

  public long getMinValue() {
    for (int i = 0, n = bounds.length; i < n; ++i) {
      if (events[i] > 0) {
        return i > 0 ? bounds[i - 1] : bounds[0];
      }
    }
    return -1;
  }

  public long getMaxValue() {
    return bounds[bounds.length - 1];
  }

  public float mean() {
    return Statistics.mean(bounds, events);
  }

  public float median() {
    return percentile(50.0f);
  }

  public float percentile(final float p) {
    return Statistics.percentile(p, bounds, events, getMaxValue(), getNumEvents());
  }

  @Override
  public String toString() {
    if (events == null || bounds == null) return "null";

    final StringBuilder sb = new StringBuilder(bounds.length * 8);
    sb.append('{');
    for (int i = 0; i < bounds.length; ++i) {
      if (i > 0) sb.append(", ");
      sb.append(bounds[i]).append(": ").append(events[i]);
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public JsonElement toJson() {
    final JsonObject json = new JsonObject();
    if (bounds != null) {
      json.add("bounds", JsonUtil.newJsonArray(bounds));
      json.add("events", JsonUtil.newJsonArray(events));
    }
    return json;
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
    final long numEvents = getNumEvents();
    if (numEvents == 0) return report.append("(no data)\n");

    report.append("Count: ").append(HumansUtil.humanCount(numEvents));
    report.append(" Median: ").append(humanConverter.toHuman(Math.round(median())));
    report.append('\n');
    report.append("Min: ").append(humanConverter.toHuman(getMinValue()));
    report.append(" Mean: ").append(humanConverter.toHuman(Math.round(mean())));
    report.append(" Max: ").append(humanConverter.toHuman(getMaxValue()));
    report.append('\n');
    report.append("Percentiles: P50: ").append(humanConverter.toHuman(Math.round(percentile(50))));
    report.append(" P75: ").append(humanConverter.toHuman(Math.round(percentile(75))));
    report.append(" P99: ").append(humanConverter.toHuman(Math.round(percentile(99))));
    report.append(" P99.9: ").append(humanConverter.toHuman(Math.round(percentile(99.9f))));
    report.append(" P99.99: ").append(humanConverter.toHuman(Math.round(percentile(99.99f))));
    report.append("\n--------------------------------------------------------------------------------\n");

    final double mult = 100.0 / numEvents;
    long cumulativeSum = 0;
    for (int b = 0; b < bounds.length; ++b) {
      final long bucketValue = events[b];
      if (bucketValue == 0) continue;

      cumulativeSum += bucketValue;
      report.append(String.format("[%15s, %15s) %7s %7.3f%% %7.3f%% ",
          humanConverter.toHuman((b == 0) ? 0 : bounds[b - 1]),
          humanConverter.toHuman(bounds[b]),
          HumansUtil.humanCount(bucketValue),
          (mult * bucketValue),
          (mult * cumulativeSum)));

      // Add hash marks based on percentage
      final long marks = Math.round(mult * bucketValue / 5 + 0.5);
      StringUtil.append(report, '#', marks);
      report.append('\n');
    }
    return report;
  }
}