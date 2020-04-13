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

import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public class TopKData implements TelemetryCollectorData {
  private final List<String> HEADER = Arrays.asList("", "Max Timestamp", "Max", "Min", "Avg", "Freq", "Trace Ids");

  public static final TopKData EMPTY = new TopKData(null);

  private final TopEntry[] entries;

  public TopKData(final TopEntry[] entries) {
    this.entries = entries;
  }

  @Override
  public JsonElement toJson() {
    final JsonArray json = new JsonArray();
    for (int i = 0, n = ArrayUtil.length(entries); i < n; ++i) {
      final TopEntry entry = entries[i];
      final JsonObject jsonEntry = new JsonObject();
      jsonEntry.addProperty("key", entry.key);
      jsonEntry.addProperty("max_ts", entry.ts);
      jsonEntry.addProperty("max", entry.vMax);
      jsonEntry.addProperty("min", entry.vMin);
      jsonEntry.addProperty("sum", entry.vSum);
      jsonEntry.addProperty("freq", entry.freq);
      json.add(jsonEntry);
    }
    return json;
  }

  @Override
  public StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter) {
    if (entries == null) return report.append("(no data)\n");

    boolean hasTraceIds = false;
    for (int i = 0; i < entries.length; ++i) {
      if (ArrayUtil.isNotEmpty(entries[i].traceIds)) {
        hasTraceIds = true;
        break;
      }
    }

    final HumansTableView table = new HumansTableView();
    table.addColumns(HEADER, 0, HEADER.size() - (hasTraceIds ? 0 : 1));
    for (int i = 0; i < entries.length; ++i) {
      final TopEntry entry = entries[i];
      table.addRow(Arrays.asList(entry.key, HumansUtil.humanDate(entry.ts),
        humanConverter.toHuman(entry.vMax), humanConverter.toHuman(entry.vMin),
        humanConverter.toHuman(entry.vSum / entry.freq), HumansUtil.humanCount(entry.freq),
        ArrayUtil.isNotEmpty(entry.traceIds) ? Arrays.toString(entry.traceIds) : ""));
    }
    return table.addHumanView(report.append('\n'));
  }

  public static final class TopEntry {
    private final String key;
    private final long vMax;
    private final long vMin;
    private final long vSum;
    private final long freq;
    private final long ts;
    private final String[] traceIds;

    public TopEntry(String key, long ts, long vMax, long vMin, long vSum, long freq, String[] traceIds) {
      this.key = key;
      this.vMax = vMax;
      this.vMin = vMin;
      this.vSum = vSum;
      this.freq = freq;
      this.ts = ts;
      this.traceIds = traceIds;
    }
  }
}
