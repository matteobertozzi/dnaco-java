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

import java.util.concurrent.ConcurrentHashMap;

import tech.dnaco.strings.StringUtil;

public final class TelemetryCollectorRegistry extends TelemetryCollectorGroup {
  public static final TelemetryCollectorRegistry INSTANCE = new TelemetryCollectorRegistry();

  private final ConcurrentHashMap<String, TelemetryCollectorGroup> tenantMap = new ConcurrentHashMap<>();

  private TelemetryCollectorRegistry() {
    super("telemetry_registry");
  }

  public void updateMemoryUsage() {
    JvmGcMetrics.INSTANCE.collect();
  }

  public TelemetryCollectorGroup getTenantGroup(final String tenantId) {
    final TelemetryCollectorGroup group = tenantMap.get(tenantId);
    if (group != null) return group;

    final TelemetryCollectorGroup newGroup = new TelemetryCollectorGroup(tenantId);
    final TelemetryCollectorGroup oldGroup = tenantMap.putIfAbsent(tenantId, newGroup);
    return oldGroup != null ? oldGroup : newGroup;
  }

  public String humanReport(final StringBuilder report) {
    jvmHumanReport(report);
    return super.humanReport(report);
  }

  public String humanReport(final StringBuilder report, final String key) {
    if (StringUtil.isEmpty(key)) return humanReport(report);

    jvmHumanReport(report);

    final TelemetryCollectorGroup tenantGroup = tenantMap.get(key);
    if (tenantGroup != null) return tenantGroup.humanReport(report);

    return super.humanReport(report, key);
  }

  private void jvmHumanReport(final StringBuilder report) {
    JvmMetrics.INSTANCE.getSnapshot().toHumanReport(report, null);
    report.append('\n');
    JvmGcMetrics.INSTANCE.getSnapshot().toHumanReport(report, null);
    report.append('\n');
    JvmThreadsMetrics.INSTANCE.getSnapshot().toHumanReport(report, null);
    report.append('\n');
  }
}
