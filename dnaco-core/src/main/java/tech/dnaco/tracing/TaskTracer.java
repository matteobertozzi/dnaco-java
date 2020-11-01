package tech.dnaco.tracing;

import com.google.gson.JsonObject;

import tech.dnaco.logging.LogUtil;

public class TaskTracer extends SimpleTraceSpan {

  public TaskTracer(final Thread thread, final String label) {
    this(thread, 0, label);
  }

  public TaskTracer(final Thread thread, final long parentId, final String label) {
    super(thread, parentId, LogUtil.nextTraceId(), label);
  }

  @Override
  public void close() {
    super.close();

    // remove the task from the Tracer
    Tracer.closeTask(getThread());
  }

  @Override
  protected void addToJson(JsonObject json) {
  }

  public static void main(String[] args) throws Exception {
    try (TaskTracer tracer = Tracer.newTask("foo")) {
      try (TraceSpan span = Tracer.newSpan("bar")) {
        // foo
      }
    }
  }
}