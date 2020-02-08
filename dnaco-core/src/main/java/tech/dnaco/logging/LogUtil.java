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

package tech.dnaco.logging;

import java.util.concurrent.atomic.AtomicLong;

import tech.dnaco.strings.BaseN;

public final class LogUtil {
  private LogUtil() {
    // no-op
  }

  // ===============================================================================================
  //  Logger TraceId related
  // ===============================================================================================
  private static final AtomicLong traceId = new AtomicLong(System.currentTimeMillis() - 1577836800L);
  public static long nextTraceId() {
    return traceId.incrementAndGet();
  }

  public static String toTraceId(final long traceId) {
    return BaseN.encodeBase58(traceId);
  }

  public static long fromTraceId(final String traceId) {
    return BaseN.decodeBase58(traceId);
  }

  // ===============================================================================================
  //  Log Levels related
  // ===============================================================================================
  private static final LogLevel[] LOG_LEVELS = LogLevel.values();
  public static LogLevel levelFromOrdinal(final int ordinal) {
    return LOG_LEVELS[ordinal];
  }

  public enum LogLevel {
    ALWAYS,
    FATAL,      // system is unusable
    ALERT,      // action must be taken immediately
    CRITICAL,   // critical conditions
    ERROR,      // error conditions
    WARNING,    // warning conditions
    INFO,       // informational
    DEBUG,      // debug-level messages
    TRACE,      // very verbose debug level messages
  }
}
