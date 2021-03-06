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

import java.util.List;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.util.Serialization.SerializeWithSnakeCase;

public class TopKData implements TelemetryCollectorData {
  private static final List<String> HEADER = List.of("", "Max Timestamp", "Max", "Min", "Avg", "Freq", "Trace Ids");

  public static final TopKData EMPTY = new TopKData(null);

  private final TopEntry[] entries;

  public TopKData(final TopEntry[] entries) {
    this.entries = entries;
  }

  private boolean hasTraceIds() {
    for (int i = 0, n = ArrayUtil.length(entries); i < n; ++i) {
      if (ArrayUtil.isNotEmpty(entries[i].traceIds)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
    if (entries == null) return report.append("(no data)\n");

    final boolean hasTraceIds = hasTraceIds();

    final HumansTableView table = new HumansTableView();
    table.addColumns(HEADER, 0, HEADER.size() - (hasTraceIds ? 0 : 1));
    for (int i = 0; i < entries.length; ++i) {
      final TopEntry entry = entries[i];

      final String traceIds;
      if (hasTraceIds) {
        final StringBuilder traces = new StringBuilder();
        for (int k = 0, kN = ArrayUtil.length(entry.traceIds); k < kN; ++k) {
          if (k > 0) traces.append(", ");
          traces.append(entry.traceIds[k]);
        }
        traceIds = traces.toString();
      } else {
        traceIds = "";
      }

      table.addRow(List.of(entry.key, HumansUtil.humanDate(entry.ts),
        humanConverter.toHuman(entry.max), humanConverter.toHuman(entry.min),
        humanConverter.toHuman(entry.sum / entry.freq), HumansUtil.humanCount(entry.freq),
        traceIds));
    }
    return table.addHumanView(report.append('\n'));
  }

  @SerializeWithSnakeCase
  public static final class TopEntry {
    private final String key;
    private final long max;
    private final long min;
    private final long sum;
    private final long freq;
    private final long ts;
    private final String[] traceIds;

    public TopEntry(final String key, final long ts, final long vMax, final long vMin, final long vSum, final long freq, final String[] traceIds) {
      this.key = key;
      this.max = vMax;
      this.min = vMin;
      this.sum = vSum;
      this.freq = freq;
      this.ts = ts;
      this.traceIds = traceIds;
    }
  }
}
