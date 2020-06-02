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

import java.lang.System.Logger.Level;
import java.text.MessageFormat;
import java.util.ResourceBundle;
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

  // ===============================================================================================
  //  System.Logger related
  // ===============================================================================================
  private static LogLevel fromJavaLevel(final Level level) {
    switch (level) {
      case ALL:     return LogLevel.ALWAYS;
      case DEBUG:   return LogLevel.DEBUG;
      case ERROR:   return LogLevel.ERROR;
      case INFO:    return LogLevel.INFO;
      case OFF:     return LogLevel.FATAL;
      case TRACE:   return LogLevel.TRACE;
      case WARNING: return LogLevel.WARNING;
    }
    throw new UnsupportedOperationException("unsupported log level " + level);
  }

  public static final class SystemLogger implements System.Logger {
    static {
      Logger.EXCLUDE_CLASSES.add(System.Logger.class.getName());
      Logger.EXCLUDE_CLASSES.add(SystemLogger.class.getName());
      Logger.EXCLUDE_CLASSES.add("jdk.internal.logger.AbstractLoggerWrapper");
      Logger.EXCLUDE_CLASSES.add("jdk.internal.net.http.common.DebugLogger");
      Logger.EXCLUDE_CLASSES.add("jdk.internal.net.http.common.Logger");
    }

    @Override
    public String getName() {
      return "dnaco-logger";
    }

    @Override
    public boolean isLoggable(Level level) {
      return Logger.isEnabled(fromJavaLevel(level));
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
      if (level.compareTo(Level.WARNING) >= 0) {
        final String text = bundle != null ? bundle.getString(msg) : msg;
        Logger.log(fromJavaLevel(level), thrown, text, null);
      }
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String format, Object... params) {
      if (level.compareTo(Level.WARNING) >= 0) {
        final String text = bundle != null ? bundle.getString(format) : format;
        Logger.log(fromJavaLevel(level), null, MessageFormat.format(text, params), null);
      }
    }
  }

  public static final class SystemLoggerFinder extends System.LoggerFinder {
    @Override
    public System.Logger getLogger(String name, Module module) {
      return new SystemLogger();
    }
  }
}
