package tech.dnaco.telemetry;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class CounterMap implements TelemetryCollector {
  private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<String, LongAdder>();

  public void inc(final String key) {
    inc(key, 1);
  }

  public void inc(final String key, final long amount) {
    LongAdder adder = counters.get(key);
    if (adder == null) {
      adder = counters.computeIfAbsent(key, k -> new LongAdder());
    }
    adder.add(amount);
  }

  public void set(final String key, final long value) {
    LongAdder adder = counters.get(key);
    if (adder == null) {
      adder = counters.computeIfAbsent(key, k -> new LongAdder());
    }
    adder.reset();
    adder.add(value);
  }

  public long get(final String key) {
    final LongAdder adder = counters.get(key);
    return adder != null ? adder.sum() : 0;
  }

  public void clear(final String key) {
    final LongAdder adder = counters.get(key);
    if (adder != null) adder.reset();
  }

  public void remove(final String key) {
    counters.remove(key);
  }

  public void clear() {
    counters.clear();
  }

  public Set<String> getKeys() {
    return counters.keySet();
  }

  public int size() {
    return counters.size();
  }

  public boolean isEmpty() {
    return counters.isEmpty();
  }

  public boolean isNotEmpty() {
    return !counters.isEmpty();
  }

  @Override
  public String getType() {
    return "COUNTER_MAP";
  }

  @Override
  public TelemetryCollectorData getSnapshot() {
    // TODO Auto-generated method stub
    return null;
  }
}
