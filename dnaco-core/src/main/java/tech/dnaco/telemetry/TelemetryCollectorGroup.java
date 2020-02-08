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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.strings.StringUtil;

public class TelemetryCollectorGroup implements TelemetryCollector {
  private final HashMap<String, CollectorInfo> collectorMap = new HashMap<>();
  private final String name;

  public TelemetryCollectorGroup(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String humanReport() {
    return humanReport(new StringBuilder());
  }

  public String humanReport(final StringBuilder report) {
    final ArrayList<String> keys = new ArrayList<>(collectorMap.keySet());
    Collections.sort(keys);

    for (String key: keys) {
      final CollectorInfo collectorInfo = collectorMap.get(key);
      report.append("--- ").append(collectorInfo.label).append(" ---\n");
      report.append(collectorInfo.name).append(": ");
      collectorInfo.getSnapshot().toHumanReport(report, collectorInfo.humanConverter);
      report.append('\n');
    }
    System.out.println(report);
    return report.toString();
  }

  public JsonObject toJson() {
    final JsonObject json = new JsonObject();
    for (CollectorInfo collectorInfo: collectorMap.values()) {
      JsonElement snapshot = collectorInfo.getSnapshot().toJson();
      json.add(collectorInfo.name, snapshot);
    }
    return json;
  }

  public <T extends TelemetryCollector> T register(final String name, final String label, final String help,
      final HumanLongValueConverter humanConverter, final T collector) {
    final CollectorInfo info = new CollectorInfo(name, label, help, humanConverter, collector);
    collectorMap.put(info.name, info);
    return collector;
  }

  @Override
  public String getType() {
    return "GROUP";
  }

  @Override
  public TelemetryCollectorData getSnapshot() {
    return new GroupData();
  }

  private static final class GroupData implements TelemetryCollectorData {
    @Override
    public JsonElement toJson() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter) {
      // TODO Auto-generated method stub
      return null;
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
      json.addProperty("unit", humanConverter.getHumanType());
      json.addProperty("help", help);
      json.add("data", snapshot.toJson());
      return json;
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
