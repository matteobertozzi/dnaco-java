package tech.dnaco.tracing;

public final class NoOpTraceSpan implements TraceSpan {
  public static final NoOpTraceSpan INSTANCE = new NoOpTraceSpan();

	@Override
	public void close() throws Exception {
		// no-op
	}

	@Override
	public long getParentId() {
		return 0;
	}

	@Override
	public long getTraceId() {
		return 0;
	}

	@Override
	public void addData(String key, Object value) {
		// no-op
	}
}