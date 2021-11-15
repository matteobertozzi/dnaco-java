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

import java.util.ArrayList;

import tech.dnaco.logging.Logger;

public final class Tracer {
  static {
    Logger.EXCLUDE_CLASSES.add(Tracer.class.getName());
  }

  private static TracingProvider provider = new NoOpTracingProvider();

  private static final ThreadLocal<RootSpan> localRootSpan = new ThreadLocal<>();
  private static final ThreadLocal<ArrayList<Span>> localSpan = ThreadLocal.withInitial(ArrayList::new);

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
  public static Span newTask() {
    return newSubTask(provider.newTraceId());
  }

  public static Span newTask(final String label) {
    final Span span = newTask();
    span.setLabel(label);
    return span;
  }

  public static Span getCurrentTask() {
    return localRootSpan.get();
  }

  public static TraceId getCurrentTraceId() {
    final Span task = getCurrentTask();
    if (task != null) return task.getTraceId();

    final Span span = getCurrentSpan();
    if (span != null) return span.getTraceId();

    return TraceId.NULL_TRACE_ID;
  }

  public static Span newSubTask(final TraceId traceId) {
    return newSubTask(traceId, null);
  }

  public static Span newSubTask(final String strTraceId, final String strParentSpanId) {
    final TraceId traceId = TraceId.fromString(strTraceId);
    final SpanId spanId = SpanId.fromString(strParentSpanId);
    return (traceId == null) ? newTask() : newSubTask(traceId, spanId);
  }

  public static Span newSubTask(final TraceId traceId, final SpanId parentSpanId) {
    final RootSpan rootSpan = new RootSpan(traceId, parentSpanId, provider.newSpanId());
    localRootSpan.set(rootSpan);
    return rootSpan;
  }

  // ================================================================================
  //  Span related
  // ================================================================================
  public static Span getCurrentSpan() {
    final ArrayList<Span> spanStack = localSpan.get();
    return spanStack.isEmpty() ? null : spanStack.get(spanStack.size() - 1);
  }

  public static SpanId getCurrentSpanId() {
    final Span span = getCurrentSpan();
    return span != null ? span.getSpanId() : SpanId.NULL_SPAN_ID;
  }

  protected static void addSpan(final Span span) {
    if (span instanceof final RootSpan rootSpan) {
      TaskMonitor.INSTANCE.addRunningTask(rootSpan);
      localRootSpan.set(rootSpan);
    }
    localSpan.get().add(span);
  }

  protected static void closeSpan(final Span span) {
    final ArrayList<Span> spanStack = localSpan.get();
    final Span lastSpan = spanStack.remove(spanStack.size() - 1);
    if (lastSpan != span) {
      throw new IllegalArgumentException("expected " + span + " to be the current span. check for missing span.close(): " + spanStack);
    }
    provider.addSpanTraces(span);

    if (!spanStack.isEmpty() && spanStack.size() % 1000 == 0) {
      Logger.error("LEAK Too many spans in the stack: {}", spanStack.size());
    }

    if (span instanceof RootSpan rootSpan) {
      if (localRootSpan.get() != rootSpan) {
        throw new IllegalArgumentException("expected " + rootSpan + " to be the current span. " + localRootSpan.get());
      }

      TaskMonitor.INSTANCE.addCompletedTask(rootSpan);

      // find the previous RootSpan
      rootSpan = null;
      for (int i = spanStack.size() - 1; i >= 0; --i) {
        final Span openSpan = spanStack.get(i);
        if (openSpan instanceof RootSpan) {
          rootSpan = (RootSpan)openSpan;
          break;
        }
      }
      localRootSpan.set(rootSpan);
    }
  }
}
