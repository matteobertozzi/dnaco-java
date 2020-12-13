package tech.dnaco.telemetry;

import java.util.concurrent.atomic.AtomicLong;

public class Gauge implements TelemetryCollector {
  private final AtomicLong counter = new AtomicLong();

  public long inc() {
    return counter.incrementAndGet();
  }

  public long inc(final long amount) {
    return counter.addAndGet(amount);
  }

  public long dec() {
    return counter.decrementAndGet();
  }

  public long dec(final long amount) {
    return counter.addAndGet(-amount);
  }

  public void set(final long newValue) {
    counter.set(newValue);
  }

  public long get() {
    return counter.get();
  }

  public long getAndReset() {
    return counter.getAndSet(0);
  }

	@Override
	public String getType() {
		return "GAUGE";
	}

	@Override
	public TelemetryCollectorData getSnapshot() {
		return new GaugeData(counter.get());
	}
}