/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.tracing;

public final class Tracer {
  private static TracingProvider provider = null;

  private static final ThreadLocal<TaskTracer> localTaskTracer = new ThreadLocal<>();
  private static final ThreadLocal<Span> localSpan = new ThreadLocal<>();

  private Tracer() {
    // no-op
  }

  // ================================================================================
  //  Provider related
  // ================================================================================
  public static void setProvider(final TracingProvider provider) {
    Tracer.provider = provider;
  }

  @SuppressWarnings("unchecked")
  public static <T extends TracingProvider> T getProvider() {
    return (T) provider;
  }

  // ================================================================================
  //  Task related
  // ================================================================================
  public static TaskTracer newTask() {
    final TaskTracer taskTracer = new TaskTracer(provider.newTraceId());
    localTaskTracer.set(taskTracer);
    return taskTracer;
  }

  public static TaskTracer getCurrentTask() {
    return localTaskTracer.get();
  }

  public static TraceId getCurrentTraceId() {
    final TaskTracer task = getCurrentTask();
    return task != null ? task.getTraceId() : TraceId.NULL_TRACE_ID;
  }

  public static TaskTracer getTask(final TraceId traceId) {
    TaskTracer taskTracer = localTaskTracer.get();
    if (taskTracer != null) return taskTracer;

    taskTracer = new TaskTracer(traceId);
    localTaskTracer.set(taskTracer);
    return taskTracer;
  }

  protected static void closeTask(final TaskTracer taskTracer) {
    if (localTaskTracer.get() == taskTracer) {
      localTaskTracer.remove();
    }
    provider.addTaskTraces(taskTracer);
  }

  // ================================================================================
  //  Span related
  // ================================================================================
  public static Span getCurrentSpan() {
    return localSpan.get();
  }

  public static SpanId getCurrentSpanId() {
    final Span span = getCurrentSpan();
    return span != null ? span.getSpanId() : SpanId.NULL_SPAN_ID;
  }

  protected static void setLocalSpan(final Span span) {
    localSpan.set(span);
  }

  protected static void closeSpan(final Span span) {
    if (localSpan.get() == span) {
      localSpan.remove();
    }
    provider.addSpanTraces(span);
  }
}
