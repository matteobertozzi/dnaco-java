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

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public class TimeRangeCounterData implements TelemetryCollectorData {
  private final long window;
  private final long lastInterval;
  private final long[] counters;

  public TimeRangeCounterData(final long lastInterval, final long window, final long[] counters) {
    this.window = window;
    this.lastInterval = lastInterval;
    this.counters = counters;
  }

  public long getWindow() {
    return window;
  }

  public long getFirstInterval() {
    return lastInterval - (counters.length * window);
  }

  public long getLastInterval() {
    return lastInterval + window;
  }

  public long[] getCounters() {
    return counters;
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
    report.append("window ").append(HumansUtil.humanTimeMillis(window));
    report.append(" - ").append(HumansUtil.localFromEpochMillis(getFirstInterval()));
    report.append(" - [");
    for (int i = 0, n = ArrayUtil.length(counters); i < n; ++i) {
      if (i > 0) report.append(',');
      report.append(humanConverter.toHuman(counters[i]));
    }
    report.append("] - ");
    report.append(HumansUtil.localFromEpochMillis(getLastInterval()));
    report.append('\n');
    return report;
  }
}
