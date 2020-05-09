/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.tracing;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TraceSession {
  private static final AtomicLong SEQUENCE = new AtomicLong(0);

  private transient final AtomicInteger refCount = new AtomicInteger(0);

  private final List<Tracer> tracers = new CopyOnWriteArrayList<>();
  private final String sessionId;
  private final long startTime;
  private final long seqId;

  private long endTime = -1;

  TraceSession(final String sessionId) {
    this.sessionId = sessionId;
    this.startTime = System.currentTimeMillis();
    this.seqId = SEQUENCE.incrementAndGet();
  }

  public String getSessionId() {
    return sessionId;
  }

  public long getSeqId() {
    return seqId;
  }

  public long getElapsedTime() {
    if (endTime < 0) {
      System.out.println("endTime=" + endTime);
      return System.currentTimeMillis() - startTime;
    }
    return endTime - startTime;
  }

  List<Tracer> getTracers() {
    return tracers;
  }

  void beginTraceSpan(final Tracer tracer) {
    refCount.incrementAndGet();
    tracers.add(tracer);
  }

  void endTraceSpan(final Tracer tracer) {
    final int refs = refCount.decrementAndGet();
    if (refs < 0) {
      throw new IllegalStateException("unexpected negative ref-count for trace session " + sessionId);
    }

    if (refs == 0) {
      tracers.sort(Tracer.SORT_COMPARATOR);
      this.endTime = System.currentTimeMillis();
      Traces.endSession(this);
    }
  }

  @Override
  public String toString() {
    return "TraceSession [sessionId=" + sessionId + ", traces=" + tracers.size() + "]";
  }

  public static final Comparator<TraceSession> DURATION_COMPARATOR = new Comparator<>() {
    @Override public int compare(final TraceSession a, final TraceSession b) {
      return Long.compare(a.getElapsedTime(), b.getElapsedTime());
    }
  };
}
