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
}
