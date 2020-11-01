package tech.dnaco.tracing;

public interface TraceSpan extends AutoCloseable {
  long getParentId();
  long getTraceId();

  void addData(String key, Object value);
}