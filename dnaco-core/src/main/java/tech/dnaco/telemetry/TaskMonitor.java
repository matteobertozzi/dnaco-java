package tech.dnaco.telemetry;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import tech.dnaco.logging.LogUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil;

public class TaskMonitor {
  public static final TaskMonitor INSTANCE = new TaskMonitor();

  private static final Object DUMMY = new Object();

  private static final ConcurrentHashMap<ActiveTask, Object> activeTasks = new ConcurrentHashMap<>();

  private TaskMonitor() {
    // no-op
  }

  public ActiveTask addTask(final String name) {
    return addTask(name, -1);
  }

  public ActiveTask addTask(final String name, final long queueTime) {
    return addTask(Thread.currentThread(), name, queueTime);
  }

  public ActiveTask addTask(final Thread thread, final String name, final long queueTime) {
    final ActiveTask task = new ActiveTask(thread, name, queueTime);
    activeTasks.put(task, DUMMY);
    return task;
  }

  protected void removeTask(final ActiveTask task) {
    activeTasks.remove(task);

    final long execTime = System.nanoTime() - task.startTime;
    Logger.debug("task {} completed in {}", task.getName(), HumansUtil.humanTimeNanos(execTime));
  }

  public void addToHumanReport(final StringBuilder report) {
    final ArrayList<ActiveTask> tasks = new ArrayList<>(activeTasks.keySet());
    tasks.sort((a, b) -> Long.compare(b.startTime, a.startTime));

    final long now = System.nanoTime();
    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "TraceId", "Queue Time", "Run Time", "Name");
    for (ActiveTask task: tasks) {
      table.addRow(task.getThread().getName(),
        LogUtil.toTraceId(task.getTraceId()),
        task.hasQueueTime() ? HumansUtil.humanTimeNanos(task.getQueueTime()) : "",
        HumansUtil.humanTimeNanos(task.getRunTime(now)),
        task.getName());
    }

    table.addHumanView(report);
  }

  public static class ActiveTask implements AutoCloseable {
    private final LoggerSession session;
    private final Thread thread;
    private final String name;
    private final long queueTime;
    private final long startTime;

    protected ActiveTask(final Thread thread, final String name, final long queueTime) {
      this.session = Logger.getSession();
      this.thread = thread;
      this.name = name;
      this.queueTime = queueTime;
      this.startTime = System.nanoTime();
    }

    @Override
    public void close() {
      TaskMonitor.INSTANCE.removeTask(this);
    }

    public String getName() {
      return name;
    }

    public Thread getThread() {
      return thread;
    }

    public long getStartTime() {
      return startTime;
    }

    public boolean hasQueueTime() {
      return queueTime >= 0;
    }

    public long getQueueTime() {
      return queueTime;
    }

    public long getRunTime(final long now) {
      return now - startTime;
    }

    public long getTraceId() {
      return session.getTraceId();
    }
  }
}
