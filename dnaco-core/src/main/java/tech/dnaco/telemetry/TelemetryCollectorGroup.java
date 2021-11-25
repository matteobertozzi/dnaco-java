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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.strings.StringUtil;

public class TelemetryCollectorGroup implements TelemetryCollector {
  private final ConcurrentSkipListMap<String, CollectorInfo> collectorMap = new ConcurrentSkipListMap<>();

  public Set<String> keys() {
    return collectorMap.keySet();
  }

  public Collection<CollectorInfo> getMetricsInfo() {
    return collectorMap.values();
  }

  @SuppressWarnings("unchecked")
  public <T extends TelemetryCollector> T get(final String name) {
    final CollectorInfo info = collectorMap.get(name);
    return (info != null) ? (T) info.collector : null;
  }

  @SuppressWarnings("unchecked")
  public <T extends TelemetryCollector> T remove(final String name) {
    final CollectorInfo info = collectorMap.remove(name);
    return (info != null) ? (T) info.collector : null;
  }

  public String humanReport() {
    return humanReport(new StringBuilder(1 << 20));
  }

  public String humanReport(final String key) {
    return humanReport(new StringBuilder(64 << 10), key);
  }

  public String humanReport(final StringBuilder report) {
    for (final Entry<String, CollectorInfo> entry: collectorMap.entrySet()) {
      humanReport(report, entry.getValue());
    }
    return report.toString();
  }

  public String humanReport(final StringBuilder report, final String key) {
    if (StringUtil.isEmpty(key)) return humanReport(report);

    for (final Entry<String, CollectorInfo> entry: collectorMap.tailMap(key).entrySet()) {
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
      collectorInfo.asGroup().humanReport(report);
    } else {
      report.append("--- ").append(collectorInfo.label).append(" ---\n");
      report.append(collectorInfo.name).append(": ");
      try {
        collectorInfo.getSnapshot().toHumanReport(report, collectorInfo.humanConverter);
      } catch (final Throwable e) {
        Logger.error(e, "failed to create human report for {}", collectorInfo.name);
      }
    }
    report.append('\n');
  }

  public <T extends TelemetryCollectorGroup> T register(final String name, final String label, final String help, final T collector) {
    return register(name, label, help, null, collector);
  }

  @SuppressWarnings("unchecked")
  public <T extends TelemetryCollector> T register(final String name, final String label, final String help,
      final HumanLongValueConverter humanConverter, final T collector) {
    if (!(collector instanceof TelemetryCollectorGroup)) {
      if (humanConverter == null) {
        Logger.warn("null human converter for {} {}", name, label);
      }
    }

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
    return new GroupData(export());
  }

  public List<TelemetryCollectorExport> export() {
    final ArrayList<TelemetryCollectorExport> metrics = new ArrayList<>(collectorMap.size());
    addToExport(metrics, null);
    return metrics;
  }

  public List<TelemetryCollectorExport> export(final String key) {
    try {
      final ArrayList<TelemetryCollectorExport> metrics = new ArrayList<>(collectorMap.size());
      addToExport(metrics, key, StringUtil.isEmpty(key) ? collectorMap : collectorMap.tailMap(key));
      return metrics;
    } catch (final Throwable e) {
      Logger.error(e, "failed for key {}: {}", key, collectorMap);
      return Collections.emptyList();
    }
  }

  public void addToExport(final List<TelemetryCollectorExport> metrics, final String prefix) {
    addToExport(metrics, prefix, collectorMap);
  }

  private void addToExport(final List<TelemetryCollectorExport> metrics, final String prefix, final Map<String, CollectorInfo> collectorMap) {
    for (final CollectorInfo collectorInfo: collectorMap.values()) {
      if (collectorInfo.isGroup()) {
        final TelemetryCollectorGroup group = (TelemetryCollectorGroup) collectorInfo.collector;
        group.addToExport(metrics, StringUtil.isEmpty(prefix) ? collectorInfo.getName() : prefix + "." + collectorInfo.getName());
      } else {
        metrics.add(collectorInfo.export(prefix));
      }
    }
  }

  private static final class GroupData implements TelemetryCollectorData {
    private TelemetryCollectorExport[] metrics;

    public GroupData() {
      // no-op
    }

    private GroupData(final List<TelemetryCollectorExport> metrics) {
      this.metrics = metrics.toArray(new TelemetryCollectorExport[0]);
    }

    @Override
    public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class CollectorInfo {
    private final String name;
    private final String label;
    private final String help;
    private transient final HumanLongValueConverter humanConverter;
    private transient final TelemetryCollector collector;

    protected CollectorInfo(final String name, final String label, final String help,
        final HumanLongValueConverter humanConverter, final TelemetryCollector collector) {
      this.name = name;
      this.label = label;
      this.help = help;
      this.humanConverter = humanConverter;
      this.collector = collector;
      if (this.collector == null) {
        throw new IllegalArgumentException("collector cannot be null. name=" + name);
      }
    }

    public TelemetryCollectorExport export(final String prefix) {
      return new TelemetryCollectorExport(StringUtil.isEmpty(prefix) ? name : prefix + "." + name, label, help,
        collector.getType(), humanConverter != null ? humanConverter.getHumanType() : null,
        collector.getSnapshot());
    }

    public boolean isGroup() {
      return collector instanceof TelemetryCollectorGroup;
    }

    public TelemetryCollectorGroup asGroup() {
      return (TelemetryCollectorGroup) collector;
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

    @SuppressWarnings("unchecked")
    public <T> T getCollector() {
      return (T) collector;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!(obj instanceof final CollectorInfo other)) return false;

      return StringUtil.equals(name, other.name);
    }
  }
}
