package tech.dnaco.telemetry;

import java.util.concurrent.TimeUnit;

public class ConcurrentMaxAndAvgTimeRangeGauge extends MaxAndAvgTimeRangeGauge {
  public ConcurrentMaxAndAvgTimeRangeGauge(final long maxInterval, final long window, final TimeUnit unit) {
    super(maxInterval, window, unit);
  }

  public void clear(final long now) {
    synchronized (this) {
      super.clear(now);
    }
  }

  public void update(final long value) {
    set(System.currentTimeMillis(), value);
  }

  public void set(final long now, final long value) {
    synchronized (this) {
      super.set(System.currentTimeMillis(), value);
    }
  }
}