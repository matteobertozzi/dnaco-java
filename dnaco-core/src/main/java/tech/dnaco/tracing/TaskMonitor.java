/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.tracing;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import tech.dnaco.collections.maps.StringObjectMap;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.CounterMap;
import tech.dnaco.telemetry.TelemetryCollector;

public final class TaskMonitor {
  public static final TaskMonitor INSTANCE = new TaskMonitor();
  static {
    Logger.EXCLUDE_CLASSES.add(TaskMonitor.class.getName());
  }

  private final CopyOnWriteArrayList<Consumer<Span>> taskCompletedListeners = new CopyOnWriteArrayList<>();

  private final Set<RootSpan> runningTasks = ConcurrentHashMap.newKeySet(128);
  private final RootSpan[] recentlyCompletedTracers = new RootSpan[16];
  private final AtomicLong recentlyCompletedIndex = new AtomicLong(0);

  private final CounterMap tenantCpuTime = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .setName("tenant.cpu.time")
    .setLabel("Tenant CPU Time")
    .register(new CounterMap());

  private TaskMonitor() {
    // no-op
  }

  public void addTaskCompletedListener(final Consumer<Span> consumer) {
    this.taskCompletedListeners.add(consumer);
  }

  protected void addRunningTask(final RootSpan task) {
    runningTasks.add(task);
  }

  protected void addCompletedTask(final RootSpan task) {
    runningTasks.remove(task);

    final int index = (int) (recentlyCompletedIndex.incrementAndGet() & (recentlyCompletedTracers.length - 1));
    recentlyCompletedTracers[index] = task;

    // keep track of cpu time per tenant
    tenantCpuTime.inc(task.getAttributes().getString(TraceAttributes.TENANT_ID, "unknown"), task.getElapsedNs());

    // call listeners
    if (!taskCompletedListeners.isEmpty()) {
      for (final Consumer<Span> consumer: taskCompletedListeners) {
        consumer.accept(task);
      }
    }
  }

  public List<RootSpan> getRecentlyCompletedTasks() {
    final ArrayList<RootSpan> tasks = new ArrayList<>(recentlyCompletedTracers.length);
    for (int i = 0; i < recentlyCompletedTracers.length; ++i) {
      if (recentlyCompletedTracers[i] == null) continue;
      tasks.add(recentlyCompletedTracers[i]);
    }
    tasks.sort((a, b) -> Long.compare(b.getStartTime(), a.getStartTime()));
    return tasks;
  }

  public StringBuilder addActiveTasksToHumanReport(final StringBuilder report) {
    final ArrayList<RootSpan> activeTasks = new ArrayList<>(runningTasks);
    activeTasks.sort((a, b) -> Long.compare(b.getStartTime(), a.getStartTime()));

    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "TenantId", "TraceId", "Queue Time", "Run Time", "Name");

    final long now = System.nanoTime();
    for (final RootSpan task: activeTasks) {
      final StringObjectMap attrs = task.getAttributes();
      final long queueTime = attrs.getLong(TraceAttributes.QUEUE_TIME, -1);
      final long elapsed = now - task.getStartNs();

      table.addRow(attrs.getString(TraceAttributes.THREAD_NAME, null),
        attrs.getString(TraceAttributes.TENANT_ID, null),
        task.getTraceId() + ":" + task.getParentSpanId() + ":" + task.getSpanId(),
        queueTime >= 0 ? HumansUtil.humanTimeNanos(queueTime) : "",
        HumansUtil.humanTimeNanos(elapsed),
        attrs.getString(TraceAttributes.LABEL, task.getCallerMethod()));
    }

    return table.addHumanView(report);
  }

  public StringBuilder addRecentlyCompletedTasksToHumanReport(final StringBuilder report) {
    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "TenantId", "TraceId", "Start Time", "Queue Time", "Execution Time", "Name", "Status");

    for (final RootSpan task: getRecentlyCompletedTasks()) {
      final StringObjectMap attrs = task.getAttributes();
      final long queueTime = attrs.getLong(TraceAttributes.QUEUE_TIME, -1);

      table.addRow(attrs.getString(TraceAttributes.THREAD_NAME, null),
        attrs.getString(TraceAttributes.TENANT_ID, null),
        task.getTraceId() + ":" + task.getParentSpanId() + ":" + task.getSpanId(),
        HumansUtil.humanDate(task.getStartTime()),
        queueTime >= 0 ? HumansUtil.humanTimeNanos(queueTime) : "",
        HumansUtil.humanTimeNanos(task.getElapsedNs()),
        attrs.getString(TraceAttributes.LABEL, task.getCallerMethod()),
        task.getStatus());
    }

    return table.addHumanView(report);
  }
}
