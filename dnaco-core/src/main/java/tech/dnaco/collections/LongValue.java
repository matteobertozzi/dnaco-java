package tech.dnaco.collections;

public class LongValue {
  private long value = 0;

  public long set(final long newValue) {
    final long oldValue = this.value;
    this.value = newValue;
    return oldValue;
  }

  public long get() {
    return value;
  }

  public int intValue() {
    return Math.toIntExact(value);
  }

  public long incrementAndGet() {
    return ++value;
  }

  public long getAndIncrement() {
    return value++;
  }

  public long add(final long amount) {
    value += amount;
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
