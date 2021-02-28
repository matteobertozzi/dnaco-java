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

import tech.dnaco.collections.maps.StringObjectMap;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.time.TimeUtil;

public class Span implements AutoCloseable {
  private final StringObjectMap attributes = new StringObjectMap();
  private final ArrayList<SpanEvent> events = new ArrayList<>(0);

  private final String callerMethod;
  private final SpanId parentSpanId;
  private final SpanId spanId;
  private final long startTs;

  private Throwable exception;
  private long endTs = -1;

  protected Span(final SpanId parentSpanId, final SpanId spanId) {
    this.callerMethod = LogUtil.lookupLineClassAndMethod(2);
    this.parentSpanId = parentSpanId;
    this.spanId = spanId;
    this.startTs = TimeUtil.currentUtcMillis();
  }

  @Override
  public void close() {
    if (endTs < 0) {
      // try (Span span = Tracer.newSpan()) { ... }
      completed();
    }
    Tracer.closeSpan(this);
  }

  public SpanId getSpanId() {
    return spanId;
  }

  public SpanId getParentSpanId() {
    return parentSpanId;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public String getCallerMethod() {
    return callerMethod;
  }

  // ================================================================================
  //  Attributes related
  // ================================================================================
  public boolean hasAttributes() {
    return !attributes.isEmpty();
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
  public boolean isEnded() {
    return endTs > 0;
  }

  public boolean hasException() {
    return exception != null;
  }

  public Throwable getException() {
    return exception;
  }

  public void completed() {
    this.endTs = TimeUtil.currentUtcMillis();
  }

  public void failed(final Throwable exception) {
    this.endTs = TimeUtil.currentUtcMillis();
    this.exception = exception;
  }

  @Override
  public String toString() {
    return "Span [parentSpanId=" + parentSpanId + ", spanId=" + spanId + ", startTs=" + startTs + "]";
  }
}