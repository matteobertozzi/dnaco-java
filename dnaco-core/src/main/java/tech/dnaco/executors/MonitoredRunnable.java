package tech.dnaco.executors;

import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.Tracer;

public class MonitoredRunnable implements Runnable {
  private final Runnable runnable;

  public MonitoredRunnable(final Runnable runnable) {
    this.runnable = runnable;
  }

	@Override
	public void run() {
    try (Span span = Tracer.getCurrentTask().startSpan()) {
      runnable.run();
    }
	}
}
