package tech.dnaco.executors;

import java.util.concurrent.Executor;

public class MonitoredExecutor implements Executor {
  private final Executor executor;

  public MonitoredExecutor(final Executor executor) {
    this.executor = executor;
  }

  @Override
  public void execute(final Runnable command) {
    if (command instanceof MonitoredRunnable) {
      executor.execute(command);
    } else {
      executor.execute(new MonitoredRunnable(command));
    }
  }
}
