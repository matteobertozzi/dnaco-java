# Tracing

- Task: A full unit of work (e.g. SELECT * FROM table)
- Span: implementation detail that we want to debug


### Code
Create a new Task, set some attributes and run some code
```java
try (TaskTracer task = Tracer.newTask()) {
  task.setAttribute("tenantId", tenantId);
  ...
}
```

A Task contains one or more span. Aside passing around the TaskTracer you can always get the current local-thread task, set more attributes and start spans.
```java
Span span = Tracer.getCurrentTask().startSpan();
try {
  ...
  span.addEvent("event").setAttribute("x", 10);
  ...
  span.completed()
} catch (Exception e) {
  span.failed(e)
}
```

A method may also partecipate in a task started from someone else. Just get the task reference with the specified traceId and set attributes or start spans. You can start spans referencing a parent span. If you have the SpanId you can use that, or you can always get the current local-thread spanId.
```java
TaskTracer task = Tracer.getTask(traceId);
try (Span span = task.startSpan(Tracer.getCurrentSpanId())) {
  ...
}
```