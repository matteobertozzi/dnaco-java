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
import java.util.concurrent.atomic.AtomicLong;

import tech.dnaco.strings.HumansUtil;

public class Tracer implements AutoCloseable {
  private static final AtomicLong SEQUENCE = new AtomicLong(0);

  private transient final TraceSession session;

  private final String threadName;
  private final String caller;
  private final String debugName;
  private final long startTime;
  private final long seqId;
  private long endTime = -1;

  private Tracer(final TraceSession session, final Thread thread, final String debugName) {
    this.session = session;
    this.threadName = thread.getName();
    this.caller = findCaller(thread);
    this.debugName = debugName;
    this.startTime = System.currentTimeMillis();
    this.seqId = SEQUENCE.incrementAndGet();
  }

  @Override
  public void close() {
    this.endTime = System.currentTimeMillis();
    session.endTraceSpan(this);
  }

  public static Tracer newLocalTracer(final String debugName) {
    return newTracer(Traces.getCurrentSession(), debugName);
  }

  public static Tracer newSessionTracer(final String sessionId, final String debugName) {
    return newTracer(Traces.newSession(sessionId), debugName);
  }

  public static Tracer newTracer(final TraceSession session, final String debugName) {
    final Tracer tracer = new Tracer(session, Thread.currentThread(), debugName);
    session.beginTraceSpan(tracer);
    return tracer;
  }

  public long getTraceId() {
    return seqId;
  }

  public long getElapsedTime() {
    if (endTime < 0) {
      return System.currentTimeMillis() - startTime;
    }
    return endTime - startTime;
  }

  void addToHumanReport(final StringBuilder report) {
    report.append(HumansUtil.humanDate(startTime)).append(".").append(seqId);
    report.append(" [").append(threadName).append("] ");
    report.append(debugName).append(": ");
    report.append(caller).append(" ");
    report.append(HumansUtil.humanTimeMillis(getElapsedTime()));
  }

  private static String findCaller(final Thread thread) {
    final StackTraceElement[] stackTrace = thread.getStackTrace();
    for (int i = 4; i < stackTrace.length; ++i) {
      final StackTraceElement elem = stackTrace[i];
      if (elem.getClassName().equals(Tracer.class.getName())) continue;
      return  elem.getClassName() + "." + elem.getMethodName() + "():" + elem.getLineNumber();
    }
    throw new UnsupportedOperationException();
  }

  public static final Comparator<Tracer> SORT_COMPARATOR = new Comparator<Tracer>() {
    @Override
    public int compare(final Tracer a, final Tracer b) {
      int cmp;
      if ((cmp = a.threadName.compareTo(b.threadName)) != 0) return cmp;
      if ((cmp = Long.compare(a.startTime, b.startTime)) != 0) return cmp;
      if ((cmp = Long.compare(a.seqId, b.seqId)) != 0) return cmp;
      return 0;
    }
  };
}
