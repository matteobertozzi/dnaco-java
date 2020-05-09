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

public class MaxAndAvgTimeRangeGaugeData implements TelemetryCollectorData {
  private final long window;
  private final long lastInterval;
  private final long[] vAvg;
  private final long[] vMax;

  public MaxAndAvgTimeRangeGaugeData(final long lastInterval, final long window, final long[] vAvg, final long[] vMax) {
    this.window = window;
    this.lastInterval = lastInterval;
    this.vAvg = vAvg;
    this.vMax = vMax;
  }

  public long getWindow() {
    return window;
  }

  public long getFirstInterval() {
    return lastInterval - (vMax.length * window);
  }

  public long getLastInterval() {
    return lastInterval + window;
  }

  public long[] getAvg() {
    return vAvg;
  }

  public long[] getMax() {
    return vMax;
  }

  @Override
  public JsonElement toJson() {
    final JsonObject json = new JsonObject();
    json.addProperty("window", window);
    json.addProperty("last_interval", lastInterval);
    json.add("avg", JsonUtil.newJsonArray(vAvg));
    json.add("max", JsonUtil.newJsonArray(vMax));
    return json;
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
    report.append("window ").append(HumansUtil.humanTimeMillis(window));
    report.append(" - ").append(HumansUtil.localFromEpochMillis(getFirstInterval()));
    report.append(" - [");
    for (int i = 0, n = ArrayUtil.length(vMax); i < n; ++i) {
      if (i > 0) report.append(',');
      report.append(humanConverter.toHuman(vAvg[i])).append('/').append(humanConverter.toHuman(vMax[i]));
    }
    report.append("] - ");
    report.append(HumansUtil.localFromEpochMillis(getLastInterval()));
    report.append('\n');
    return report;
  }

  public StringBuilder toHumanChartReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
    final int numEvents = ArrayUtil.length(vMax);
    long avgSum = 0;
    long maxValue = 0;

    for (int i = 0; i < numEvents; ++i) {
      avgSum += vAvg[i];
      maxValue = Math.max(maxValue, vMax[i]);
    }
    final double mult = 100.0 / maxValue;

    report.append("window ").append(HumansUtil.humanTimeMillis(window));
    report.append(" - Max Latency ").append(humanConverter.toHuman(maxValue));
    report.append(" - Avg Latency ").append(humanConverter.toHuman(avgSum / numEvents));
    report.append('\n');
    report.append("---------------------------------------------------------------------------\n");
    long interval = getLastInterval();
    for (int i = numEvents - 1; i >= 0; --i) {
      final long avgMarks = Math.round(mult * vAvg[i] / 5 + 0.5);
      final long maxMarks = Math.round(mult * (vMax[i] - vAvg[i]) / 5 + 0.5);

      report.append(HumansUtil.humanDate(interval));
      report.append(String.format(" %7s/%-7s %6.2f%% |",
        humanConverter.toHuman(vAvg[i]),
        humanConverter.toHuman(vMax[i]),
        ((double) vMax[i] / maxValue) * 100.0f));
      StringUtil.append(report, '=', avgMarks);
      StringUtil.append(report, '#', maxMarks);
      report.append('\n');

      interval -= window;
    }
    return report;
  }

  public static void main(String[] args) {
    long[] vAvg = new long[100];
    long[] vMax = new long[100];
    for (int i = 0; i < vMax.length; ++i) {
      vAvg[i] = Math.round(Math.random() * 100);
      vMax[i] = Math.round(vAvg[i] + Math.random() * 50);
    }

    MaxAndAvgTimeRangeGaugeData data = new MaxAndAvgTimeRangeGaugeData(System.currentTimeMillis(), 5000, vAvg, vMax);
    System.out.println(data.toHumanChartReport(new StringBuilder(), HumansUtil.HUMAN_TIME_NANOS));
  }
}
