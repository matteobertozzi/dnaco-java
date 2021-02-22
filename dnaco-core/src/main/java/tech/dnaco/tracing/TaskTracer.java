package tech.dnaco.tracing;

import tech.dnaco.data.JsonFormatUtil.JsonObject;
import tech.dnaco.logging.LogUtil;

public class TaskTracer extends SimpleTraceSpan {
  public TaskTracer(final Thread thread, final String callerMethodAndLine, final String label) {
    this(thread, 0, callerMethodAndLine, label);
  }

  public TaskTracer(final Thread thread, final long parentId, final String callerMethodAndLine, final String label) {
    super(thread, parentId, LogUtil.nextTraceId(), callerMethodAndLine, label);
  }

  @Override
  public void close() {
    super.close();

    // remove the task from the Tracer
    Tracer.closeTask(getThread());
  }

  @Override
  protected void addToJson(final JsonObject json) {
  }

  public static void main(final String[] args) throws Exception {
    try (TaskTracer tracer = Tracer.newTask("foo")) {
      try (TraceSpan span = Tracer.newSpan("bar")) {
        // foo
      }
    }
  }
}