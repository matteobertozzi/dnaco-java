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

import tech.dnaco.collections.maps.StringObjectMap;

public class TaskTracer implements AutoCloseable {
  private final StringObjectMap attributes = new StringObjectMap();
  private final TraceId traceId;

  public TaskTracer(final TraceId traceId) {
    this.traceId = traceId;
  }

  @Override
  public void close() {
    Tracer.closeTask(this);
  }

  public TraceId getTraceId() {
    return traceId;
  }

  // ================================================================================
  //  Spans related
  // ================================================================================
  public Span startSpan() {
    return startSpan(null);
  }

  public Span startSpan(final SpanId parentSpanId) {
    final Span span = new Span(parentSpanId, Tracer.getProvider().newSpanId());
    Tracer.setLocalSpan(span);
    return span;
  }

  // ================================================================================
  //  Attributes related
  // ================================================================================
  public boolean hasAttributes() {
    return !attributes.isEmpty();
  }

  public TaskTracer setAttribute(final String key, final Object value) {
    this.attributes.put(key, value);
    return this;
  }

  @Override
  public String toString() {
    return "TaskTracer [traceId=" + traceId + "]";
  }
}
