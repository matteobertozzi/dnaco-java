package tech.dnaco.telemetry;

import java.util.Arrays;

public abstract class TimeRangeCounterIterator extends TimeRangeIterator {
  private final long[] values;

  public TimeRangeCounterIterator(final long window) {
    this(DEFAULT_NBLOCK, window);
  }

  public TimeRangeCounterIterator(final int nBlocks, final long window) {
    super(nBlocks, window);
    this.values = new long[nBlocks];
  }

  public long getValue() {
    return values[offset()];
  }

  public long[] values() {
    return values;
  }

  public double getFloatValue() {
    return Double.longBitsToDouble(getValue());
  }

  public void setValue(final long value) {
    values[offset()] = value;
  }

  public void incValue(final long amount) {
    values[offset()] += amount;
  }

  public void resetValues() {
    Arrays.fill(values, 0);
  }

  public void setValue(final int index, final long value) {
    values[index] = value;
  }

  public void setValue(final double value) {
    setValue(Double.doubleToLongBits(value));
  }

  protected abstract boolean loadData(final long blockTsOffset);
}
