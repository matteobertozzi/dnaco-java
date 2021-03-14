# Tracing

- Task: A full unit of work (e.g. SELECT * FROM table)
- Span: implementation detail that we want to debug


### Code
Create a new Task, set some attributes and run some code
```java
try (Span span = Tracer.newTask()) {
  span.setAttribute("tenantId", tenantId);
  ...
}
```

A Task may have one or more spans. Aside passing around the Span you can always get the current local-thread task, set more attributes and start spans.
```java
try (Span span = Tracer.getCurrentTask().startSpan()) {
  try {
    ...
    span.addEvent("event").setAttribute("x", 10);
    ...
  } catch (Exception e) {
    span.failed(e)
  }
}
```

A method may also partecipate in a task started from someone else. Just get the task reference with the specified traceId and set attributes or start spans. You can start spans referencing a parent span. If you have the SpanId you can use that, or you can always get the current local-thread spanId.
```java
try (Span span = task.newSubTask(traceId, parentSpanId)) {
  ...
}
```