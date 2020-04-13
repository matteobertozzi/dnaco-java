package tech.dnaco.telemetry;

import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

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

  private static final class GaugeData implements TelemetryCollectorData {
    private final long value;

    private GaugeData(final long value) {
      this.value = value;
    }

    @Override
    public JsonElement toJson() {
      return new JsonPrimitive(value);
    }

    @Override
    public StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter) {
      return report.append(humanConverter.toHuman(value));
    }
  }
}