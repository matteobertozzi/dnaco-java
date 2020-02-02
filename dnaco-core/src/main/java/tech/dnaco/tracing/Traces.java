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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import com.google.gson.JsonObject;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.JsonUtil;

public final class Traces {
  private static final int TRACE_BUFFER_SIZE = 32;

  private static final PriorityBlockingQueue<TraceSession> traceBuffer = new PriorityBlockingQueue<>(TRACE_BUFFER_SIZE, TraceSession.DURATION_COMPARATOR);
  private static final ConcurrentHashMap<String, TraceSession> traceSessionMap = new ConcurrentHashMap<>();
  private static final ThreadLocal<TraceSession> currentSession = new ThreadLocal<>();

  public static TraceSession newSession(final String sessionId) {
    final TraceSession session = new TraceSession(sessionId);
    final TraceSession oldSession = traceSessionMap.putIfAbsent(sessionId, session);
    if (oldSession != null) {
      throw new IllegalStateException("a trace session named '" + sessionId + "' already exists");
    }

    currentSession.set(session);
    return session;
  }

  static void endSession(final TraceSession session) {
    traceSessionMap.remove(session.getSessionId());
    if (currentSession.get() == session) {
      currentSession.remove();
    }

    if (traceBuffer.size() >= TRACE_BUFFER_SIZE) {
      traceBuffer.poll();
    }
    traceBuffer.add(session);
  }

  public static TraceSession getSession(final String sessionId) {
    return traceSessionMap.get(sessionId);
  }

  public static TraceSession getCurrentSession() {
    return currentSession.get();
  }

  public static JsonObject toJson() {
    final JsonObject json = new JsonObject();
    json.add("live", JsonUtil.toJsonTree(traceSessionMap.values()));
    json.add("traces", JsonUtil.toJsonTree(traceBuffer));
    return json;
  }

  public static String humanReport() {
    final StringBuilder report = new StringBuilder();
    for (final TraceSession entry: traceSessionMap.values()) {
      report.append("--- LIVE ").append(entry.getSessionId()).append(" ---\n");
      for (final Tracer tracer: entry.getTracers()) {
        tracer.addToHumanReport(report);
        report.append('\n');
      }
    }

    for (final TraceSession entry: traceBuffer) {
      report.append("--- ").append(entry.getSessionId());
      report.append(" (").append(HumansUtil.humanTimeMillis(entry.getElapsedTime())).append(") ---\n");
      for (final Tracer tracer: entry.getTracers()) {
        tracer.addToHumanReport(report);
        report.append('\n');
      }
    }
    return report.toString();
  }
}
