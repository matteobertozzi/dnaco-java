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

import tech.dnaco.logging.Logger;

public final class TaskMonitor {
  public static final TaskMonitor INSTANCE = new TaskMonitor();
  static {
    Logger.EXCLUDE_CLASSES.add(TaskMonitor.class.getName());
  }

  private TaskMonitor() {
    // no-op
  }

  /*
  public void addActiveTasksToHumanReport(final StringBuilder report) {
    final ArrayList<TaskTracer> activeTasks = new ArrayList<>(Tracer.getActiveTasks());
    activeTasks.sort((a, b) -> Long.compare(b.getStartTime(), a.getStartTime()));

    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "ProjectId", "TraceId", "Queue Time", "Run Time", "Name");

    final long now = System.nanoTime();
    for (final TaskTracer task: activeTasks) {
      final long queueTime = task.getLongData(TraceAttrs.TRACE_QUEUE_TIME, -1);

      table.addRow(task.getThread().getName(),
        task.getStringData(TraceAttrs.TRACE_TENANT_ID),
        LogUtil.toTraceId(task.getTraceId()),
        queueTime >= 0 ? HumansUtil.humanTimeNanos(queueTime) : "",
        HumansUtil.humanTimeNanos(task.getElapsedNs(now)),
        task.getLabel());
    }

    table.addHumanView(report);
  }

  public void addRecentlyCompletedTasksToHumanReport(final StringBuilder report) {
    final List<TaskTracer> tasks = Tracer.getRecentlyCompletedTasks();

    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "ProjectId", "TraceId", "Start Time", "Queue Time", "Execution Time", "Name");

    for (final TaskTracer task: tasks) {
      final long queueTime = task.getLongData(TraceAttrs.TRACE_QUEUE_TIME, -1);

      table.addRow(task.getThread().getName(),
        task.getStringData(TraceAttrs.TRACE_TENANT_ID),
        LogUtil.toTraceId(task.getTraceId()),
        HumansUtil.humanDate(task.getStartTime()),
        queueTime >= 0 ? HumansUtil.humanTimeNanos(queueTime) : "",
        HumansUtil.humanTimeNanos(task.getElapsedNs()),
        task.getLabel());
    }

    table.addHumanView(report);
  }
  */
}
