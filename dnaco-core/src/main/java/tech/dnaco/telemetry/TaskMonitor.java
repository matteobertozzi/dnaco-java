package tech.dnaco.telemetry;

import java.util.ArrayList;
import java.util.List;

import tech.dnaco.logging.LogUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.tracing.TaskTracer;
import tech.dnaco.tracing.TraceAttrs;
import tech.dnaco.tracing.Tracer;

public final class TaskMonitor {
  public static final TaskMonitor INSTANCE = new TaskMonitor();
  static {
    Logger.EXCLUDE_CLASSES.add(TaskMonitor.class.getName());
  }

  private TaskMonitor() {
    // no-op
  }

  public void addActiveTasksToHumanReport(final StringBuilder report) {
    final ArrayList<TaskTracer> activeTasks = new ArrayList<>(Tracer.getActiveTasks());
    activeTasks.sort((a, b) -> Long.compare(b.getStartTime(), a.getStartTime()));

    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "ProjectId", "TraceId", "Queue Time", "Run Time", "Name");

    final long now = System.nanoTime();
    for (TaskTracer task: activeTasks) {
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

    for (TaskTracer task: tasks) {
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
}
