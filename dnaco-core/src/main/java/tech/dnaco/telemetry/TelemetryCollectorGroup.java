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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.strings.StringUtil;

public class TelemetryCollectorGroup implements TelemetryCollector {
  private final ConcurrentSkipListMap<String, CollectorInfo> collectorMap = new ConcurrentSkipListMap<>();
  private final String name;

  public TelemetryCollectorGroup(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @SuppressWarnings("unchecked")
  public <T extends TelemetryCollector> T get(final String name) {
    final CollectorInfo info = collectorMap.get(name);
    return (info != null) ? (T) info.collector : null;
  }

  public String humanReport() {
    return humanReport(new StringBuilder());
  }

  public String humanReport(final String key) {
    return humanReport(new StringBuilder(), key);
  }

  public String humanReport(final StringBuilder report) {
    for (Entry<String, CollectorInfo> entry: collectorMap.entrySet()) {
      humanReport(report, entry.getValue());
    }
    return report.toString();
  }

  public String humanReport(final StringBuilder report, final String key) {
    if (StringUtil.isEmpty(key)) return humanReport(report);

    for (Entry<String, CollectorInfo> entry: collectorMap.tailMap(key).entrySet()) {
      if (!entry.getKey().startsWith(key)) continue;
      humanReport(report, entry.getValue());
    }
    return report.toString();
  }

  private void humanReport(final StringBuilder report, final CollectorInfo collectorInfo) {
    if (collectorInfo.isGroup()) {
      report.append("======================================================================\n");
      report.append(" ").append(collectorInfo.getLabel()).append(" (").append(collectorInfo.getName()).append(")").append("\n");
      if (collectorInfo.hasHelp()) report.append(" ").append(collectorInfo.getHelp()).append("\n");
      report.append("======================================================================\n");
    } else {
      report.append("--- ").append(collectorInfo.label).append(" ---\n");
      report.append(collectorInfo.name).append(": ");
    }
    collectorInfo.getSnapshot().toHumanReport(report, collectorInfo.humanConverter);
    report.append('\n');
  }

  public JsonObject toJson() {
    final JsonObject json = new JsonObject();
    for (CollectorInfo collectorInfo: collectorMap.values()) {
      final TelemetryCollectorData snapshot = collectorInfo.getSnapshot();
      json.add(collectorInfo.name, collectorInfo.toJson(snapshot));
    }
    return json;
  }

  public JsonObject toJson(final String key) {
    if (StringUtil.isEmpty(key)) return toJson();

    final JsonObject json = new JsonObject();
    for (Entry<String, CollectorInfo> entry: collectorMap.tailMap(key).entrySet()) {
      if (!entry.getKey().startsWith(key)) continue;
      final CollectorInfo collectorInfo = entry.getValue();
      final TelemetryCollectorData snapshot = collectorInfo.getSnapshot();
      json.add(collectorInfo.name, collectorInfo.toJson(snapshot));
    }
    return json;
  }

  public <T extends TelemetryCollectorGroup> T register(final String name, final String label, final String help, final T collector) {
    return register(name, label, help, null, collector);
  }

  @SuppressWarnings("unchecked")
  public <T extends TelemetryCollector> T register(final String name, final String label, final String help,
      final HumanLongValueConverter humanConverter, final T collector) {
    final CollectorInfo newInfo = new CollectorInfo(name, label, help, humanConverter, collector);
    final CollectorInfo oldInfo = collectorMap.putIfAbsent(newInfo.name, newInfo);
    return oldInfo != null ? (T) oldInfo.collector : collector;
  }

  @Override
  public String getType() {
    return "GROUP";
  }

  @Override
  public TelemetryCollectorData getSnapshot() {
    return new GroupData(toJson(), humanReport());
  }

  private static final class GroupData implements TelemetryCollectorData {
    private final JsonObject json;
    private final String report;

    private GroupData(JsonObject json, String report) {
      this.json = json;
      this.report = report;
    }

    @Override
    public JsonElement toJson() {
      return json;
    }

    @Override
    public StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter) {
      report.append('\n');
      return report.append(this.report);
    }
  }

  private static final class CollectorInfo {
    private final String name;
    private final String label;
    private final String help;
    private final HumanLongValueConverter humanConverter;
    private final TelemetryCollector collector;

    private CollectorInfo(final String name, final String label, final String help,
        final HumanLongValueConverter humanConverter, final TelemetryCollector collector) {
      this.name = name;
      this.label = label;
      this.help = help;
      this.humanConverter = humanConverter;
      this.collector = collector;
    }

    public JsonObject toJson(final TelemetryCollectorData snapshot) {
      final JsonObject json = new JsonObject();
      json.addProperty("label", label);
      json.addProperty("type", collector.getType());
      json.addProperty("help", help);
      json.add("data", snapshot.toJson());
      if (humanConverter != null) {
        json.addProperty("unit", humanConverter.getHumanType());
      }
      return json;
    }

    public boolean isGroup() {
      return collector instanceof TelemetryCollectorGroup;
    }

    public String getName() {
      return name;
    }

    public String getLabel() {
      return label;
    }

    public boolean hasHelp() {
      return StringUtil.isNotEmpty(help);
    }

    public String getHelp() {
      return help;
    }

    public TelemetryCollectorData getSnapshot() {
      return collector.getSnapshot();
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (obj instanceof CollectorInfo) return false;

      final CollectorInfo other = (CollectorInfo) obj;
      return StringUtil.equals(name, other.name);
    }
  }
}
