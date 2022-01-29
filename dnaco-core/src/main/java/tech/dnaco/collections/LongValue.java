package tech.dnaco.collections;

public class LongValue {
  private long value = 0;

  public void set(final long value) {
    this.value = value;
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

  public long add(final long amount) {
    value += amount;
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
