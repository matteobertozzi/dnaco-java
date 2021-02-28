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
import java.util.concurrent.TimeUnit;

import tech.dnaco.collections.maps.StringObjectMap;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.time.TimeUtil;

public class Span implements AutoCloseable {
  private final StringObjectMap attributes = new StringObjectMap();
  private final ArrayList<SpanEvent> events = new ArrayList<>(0);

  private final String callerMethod;
  private final TraceId traceId;
  private final SpanId parentSpanId;
  private final SpanId spanId;
  private final long startTime;
  private final long startNs;

  private Throwable exception;
  private long elapsedNs = -1;

  protected Span(final TraceId traceId, final SpanId parentSpanId, final SpanId spanId) {
    this.callerMethod = LogUtil.lookupLineClassAndMethod(2);
    this.traceId = traceId;
    this.parentSpanId = parentSpanId;
    this.spanId = spanId;
    this.startTime = TimeUtil.currentUtcMillis();
    this.startNs = System.nanoTime();
  }

  @Override
  public void close() {
    if (!isCompleted()) {
      // try (Span span = Tracer.newSpan()) { ... }
      completed();
    }
    Tracer.closeSpan(this);
  }

  public TraceId getTraceId() {
    return traceId;
  }

  public SpanId getParentSpanId() {
    return parentSpanId;
  }

  public SpanId getSpanId() {
    return spanId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return startTime + TimeUnit.NANOSECONDS.toMillis(elapsedNs);
  }

  protected long getStartNs() {
    return startNs;
  }

  public long getElapsedNs() {
    return elapsedNs;
  }

  public String getCallerMethod() {
    return callerMethod;
  }

  // ================================================================================
  //  Attributes related
  // ================================================================================
  public boolean hasAttributes() {
    return attributes.isNotEmpty();
  }

  public StringObjectMap getAttributes() {
    return attributes;
  }

  public Span setAttribute(final String key, final Object value) {
    this.attributes.put(key, value);
    return this;
  }

  // ================================================================================
  //  Events related
  // ================================================================================
  public boolean hasEvents() {
    return !events.isEmpty();
  }

  public SpanEvent addEvent(final String eventName) {
    final SpanEvent event = new SpanEvent(eventName);
    events.add(event);
    return event;
  }

  // ================================================================================
  //  Completion related
  // ================================================================================
  public boolean isCompleted() {
    return elapsedNs >= 0;
  }

  public boolean hasException() {
    return exception != null;
  }

  public Throwable getException() {
    return exception;
  }

  public void completed() {
    this.elapsedNs = System.nanoTime() - startNs;
    this.exception = null;
  }

  public void failed(final Throwable exception) {
    this.elapsedNs = System.nanoTime() - startNs;
    this.exception = exception;
  }

  @Override
  public String toString() {
    return "Span [parentSpanId=" + parentSpanId + ", spanId=" + spanId + ", startTs=" + startTime + "]";
  }
}