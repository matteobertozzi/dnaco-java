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
import java.util.List;
import java.util.concurrent.TimeUnit;

import tech.dnaco.collections.maps.StringObjectMap;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.time.TimeUtil;

public class Span implements AutoCloseable {
  static {
    Logger.EXCLUDE_CLASSES.add(Span.class.getName());
  }

  public enum SpanStatus { IN_PROGRESS, OK, SYSTEM_FAILURE, USER_FAILURE }

  private final StringObjectMap attributes = new StringObjectMap();
  private final ArrayList<SpanEvent> events = new ArrayList<>(0);

  private String threadName;
  private String callerMethod;
  private TraceId traceId;
  private SpanId parentSpanId;
  private SpanId spanId;
  private long startTime;
  private transient long startNs;

  private String tenantId;
  private String label;
  private String exception;
  private SpanStatus status;
  private long elapsedNs = -1;

  protected Span() {
    // no-op (deserialization)
  }

  protected Span(final TraceId traceId, final SpanId parentSpanId, final SpanId spanId) {
    this.callerMethod = LogUtil.lookupLineClassAndMethod(2);
    this.threadName = Thread.currentThread().getName();
    this.traceId = traceId;
    this.parentSpanId = parentSpanId;
    this.spanId = spanId;
    this.startTime = TimeUtil.currentUtcMillis();
    this.startNs = System.nanoTime();
    this.label = this.callerMethod;
    this.status = SpanStatus.IN_PROGRESS;
    Tracer.addSpan(this);
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

  public long getElapsedNs(final long nowNs) {
    return nowNs - startNs;
  }

  public String getCallerMethod() {
    return callerMethod;
  }

  public String getThreadName() {
    return threadName;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
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

  @SuppressWarnings("unchecked")
  public <T> T getAttribute(final String key, final T defaultValue) {
    final T value = (T) attributes.get(key);
    return value != null ? value : defaultValue;
  }

  public Span setAttribute(final String key, final Object value) {
    this.attributes.put(key, value);
    return this;
  }

  public Span setAttributeIfAbsent(final String key, final Object value) {
    this.attributes.putIfAbsent(key, value);
    return this;
  }

  public String getTenantId() {
    return tenantId;
  }

  public Span setTenantId(final String tenantId) {
    this.tenantId = tenantId;
    Logger.setSession(LoggerSession.newSession(tenantId, Logger.getSession()));
    return this;
  }

  public String getModule() {
    return getAttribute(TraceAttributes.MODULE, null);
  }

  public Span setModule(final String module) {
    setAttribute(TraceAttributes.MODULE, module);
    return this;
  }

  // ================================================================================
  //  Events related
  // ================================================================================
  public boolean hasEvents() {
    return !events.isEmpty();
  }

  public List<SpanEvent> getEvents() {
    return events;
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

  public SpanStatus getStatus() {
    return status;
  }

  public boolean hasException() {
    return exception != null;
  }

  public String getException() {
    return exception;
  }

  public void completed() {
    this.status = SpanStatus.OK;
    this.exception = null;
    this.elapsedNs = System.nanoTime() - startNs;
  }

  public void failed(final boolean sysFailure, final Throwable exception) {
    this.status = sysFailure ? SpanStatus.SYSTEM_FAILURE : SpanStatus.USER_FAILURE;
    this.exception = (exception != null) ? LogUtil.stackTraceToString(exception) : null;
    this.elapsedNs = System.nanoTime() - startNs;
  }

  // ================================================================================
  //  Sub Span related
  // ================================================================================
  public Span startSpan() {
    return startSpan(null);
  }

  public Span startSpan(final SpanId parentSpanId) {
    return new Span(getTraceId(), parentSpanId, Tracer.getProvider().newSpanId());
  }

  @Override
  public String toString() {
    return "Span [parentSpanId=" + parentSpanId + ", spanId=" + spanId + ", startTs=" + startTime + "]";
  }
}