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

import java.util.Formatter;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public class CounterMapData implements TelemetryCollectorData {
  public static final CounterMapData EMPTY = new CounterMapData(null, null);

  private final String[] keys;
  private final long[] events;

  public CounterMapData(final String[] keys, final long[] events) {
    this.keys = keys;
    this.events = events;
  }

  public String[] getKeys() {
    return keys;
  }

  public long[] getEvents() {
    return events;
  }

  public long getTotalEvents() {
    if (events == null) return 0;

    long total = 0;
    for (int i = 0; i < events.length; ++i) {
      total += events[i];
    }
    return total;
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report, HumanLongValueConverter humanConverter) {
    final long total = getTotalEvents();
    if (total <= 0) return report.append("(no data)\n");

    if (humanConverter == null) humanConverter = HumansUtil.HUMAN_COUNT;

    report.append("\n");
    try (Formatter formatter = new Formatter(report)) {
      for (int i = 0; i < keys.length; ++i) {
        final long value = events[i];
        report.append(" - ");
        formatter.format("%5.2f", 100 * (value / (double) total));
        report.append(" (");
        formatter.format("%7s", humanConverter.toHuman(value));
        report.append(") - ");
        report.append(keys[i]);
        report.append('\n');
      }
    }
    return report;
  }
}