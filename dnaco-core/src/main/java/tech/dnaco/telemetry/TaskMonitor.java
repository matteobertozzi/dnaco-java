package tech.dnaco.telemetry;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import tech.dnaco.logging.LogUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;

public final class TaskMonitor {
  public static final TaskMonitor INSTANCE = new TaskMonitor();

  static {
    Logger.EXCLUDE_CLASSES.add(TaskMonitor.class.getName());
    Logger.EXCLUDE_CLASSES.add(ActiveTask.class.getName());
  }

  private static final Object DUMMY = new Object();

  private static final ConcurrentHashMap<ActiveTask, Object> activeTasks = new ConcurrentHashMap<>();

  private TaskMonitor() {
    // no-op
  }

  public ActiveTask addTask(final String name) {
    return addTask(name, -1);
  }

  public ActiveTask addTask(final String name, final long queueTime) {
    return addTask(Thread.currentThread(), name, -1, queueTime);
  }

  public ActiveTask addTask(final String name, final long startTime, final long queueTime) {
    return addTask(Thread.currentThread(), name, startTime, queueTime);
  }

  public ActiveTask addTask(final Thread thread, final String name, final long queueTime) {
    return addTask(thread, name, -1, queueTime);
  }

  public ActiveTask addTask(final Thread thread, final String name, final long startTime, final long queueTime) {
    final ActiveTask task = new ActiveTask(thread, name, startTime, queueTime);
    activeTasks.put(task, DUMMY);
    return task;
  }

  protected void removeTask(final ActiveTask task) {
    activeTasks.remove(task);

    final long execTime = System.nanoTime() - task.slotStartTime;
    Logger.debug("task {} completed in {}", task.getName(), HumansUtil.humanTimeNanos(execTime));
  }

  public void addToHumanReport(final StringBuilder report) {
    final ArrayList<ActiveTask> tasks = new ArrayList<>(activeTasks.keySet());
    tasks.sort((a, b) -> Long.compare(b.slotStartTime, a.slotStartTime));

    final long now = System.nanoTime();
    final HumansTableView table = new HumansTableView();
    table.addColumns("Thread", "ProjectId", "TraceId", "Queue Time", "Slot Run Time", "Total Run Time", "Name");
    for (ActiveTask task: tasks) {
      table.addRow(task.getThread().getName(),
        task.getProjectId(),
        LogUtil.toTraceId(task.getTraceId()),
        task.hasQueueTime() ? HumansUtil.humanTimeNanos(task.getQueueTime()) : "",
        HumansUtil.humanTimeNanos(task.getSlotRunTime(now)),
        HumansUtil.humanTimeNanos(task.getTotalRunTime(now)),
        task.getName());
    }

    table.addHumanView(report);
  }

  public static class ActiveTask implements AutoCloseable {
    private final LoggerSession session;
    private final Thread thread;
    private final String name;
    private final long queueTime;
    private final long slotStartTime;
    private final long startTime;

    protected ActiveTask(final Thread thread, final String name, final long startTime, final long queueTime) {
      this.session = Logger.getSession();
      this.thread = thread;
      this.name = name;
      this.queueTime = queueTime;
      this.slotStartTime = System.nanoTime();
      this.startTime = (startTime > 0) ? startTime : slotStartTime;
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
      return slotStartTime;
    }

    public boolean hasQueueTime() {
      return queueTime >= 0;
    }

    public long getQueueTime() {
      return queueTime;
    }

    public long getSlotRunTime(final long now) {
      return now - slotStartTime;
    }

    public long getTotalRunTime(final long now) {
      return now - startTime;
    }

    public String getProjectId() {
      return StringUtil.defaultIfEmpty(session.getProjectId(), "-");
    }

    public long getTraceId() {
      return session.getTraceId();
    }
  }
}
