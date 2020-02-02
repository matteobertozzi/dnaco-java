package tech.dnaco.telemetry;

import java.util.concurrent.ConcurrentHashMap;

import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.strings.HumansUtil;

public class TaskMonitor {
  public static final TaskMonitor INSTANCE = new TaskMonitor();

  private static final ConcurrentHashMap<ActiveTask, Object> activeTasks = new ConcurrentHashMap<>();

  private TaskMonitor() {
    // no-op
  }

  public ActiveTask addTask() {
    return null;
  }

  protected void removeTask(final ActiveTask task) {
    final long execTime = System.nanoTime() - task.startTime;
    Logger.debug("task {} completed in {}", task.getName(), HumansUtil.humanTimeNanos(execTime));
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
  }
}
