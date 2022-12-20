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

package tech.dnaco.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.StackWalker.StackFrame;
import java.lang.System.Logger.Level;

public final class LogUtil {
  private LogUtil() {
    // no-op
  }

  // ===============================================================================================
  //  Log Levels related
  // ===============================================================================================
  private static final LogLevel[] LOG_LEVELS = LogLevel.values();
  public static final int LOG_LEVELS_COUNT = LOG_LEVELS.length;

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
    NEVER,
  }

  // ===============================================================================================
  //  Stack Trace to String related
  // ===============================================================================================
  public static String lookupLineClassAndMethod(final int skipFrames) {
    // Get the stack trace: this is expensive... but really useful
    // NOTE: i should be set to the first public method
    // skipFrames = 2 -> [0: lookupLogLineClassAndMethod(), 1: myLogger(), 2:userFunc()]
    final StackFrame frame = StackWalker.getInstance().walk(s ->
      s.skip(skipFrames)
      .filter(x -> !Logger.EXCLUDE_CLASSES.contains(x.getClassName()))
      .findFirst()
    ).orElseThrow();

    // com.foo.Bar.m1():11
    return getClassName(frame.getClassName()) + "." + frame.getMethodName() + "():" + frame.getLineNumber();
  }

  private static String getClassName(final String cname) {
    int index = cname.length();
    for (int i = 0; i < 2; i++) {
      final int tmp = cname.lastIndexOf('.', index - 1);
      if (tmp <= 0) break;
      index = tmp;
    }
    return cname.substring(index + 1);
  }

  public static String stackTraceToString(final StackTraceElement[] stackTrace) {
    return stackTraceToString(stackTrace, 0);
  }

  public static String stackTraceToString(final StackTraceElement[] stackTrace, final int offset) {
    if (stackTrace == null) return "";

    final StringBuilder builder = new StringBuilder((stackTrace.length - offset) * 32);
    for (int i = offset; i < stackTrace.length; ++i) {
      final StackTraceElement st = stackTrace[i];
      builder.append(getClassName(st.getClassName())).append('.').append(st.getMethodName()).append("():").append(st.getLineNumber());
      builder.append(System.lineSeparator());
    }
    return builder.toString();
  }

  public static String stackTraceToString(final Throwable exception) {
    final StringWriter writer = new StringWriter(512);
    try (PrintWriter printWriter = new PrintWriter(writer)) {
      exception.printStackTrace(printWriter);
      printWriter.flush();
      return writer.getBuffer().toString();
    }
  }

  // ===============================================================================================
  //  System.Logger related
  // ===============================================================================================
  private static LogLevel fromJavaLevel(final Level level) {
    switch (level) {
      case ALL:     return LogLevel.ALWAYS;
      case OFF:     return LogLevel.NEVER;
      case DEBUG:   return LogLevel.DEBUG;
      case ERROR:   return LogLevel.ERROR;
      case INFO:    return LogLevel.INFO;
      case TRACE:   return LogLevel.TRACE;
      case WARNING: return LogLevel.WARNING;
    }
    throw new UnsupportedOperationException("unsupported log level " + level);
  }
/*
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
    public boolean isLoggable(final Level level) {
      return Logger.isEnabled(fromJavaLevel(level));
    }

    @Override
    public void log(final Level level, final ResourceBundle bundle, final String msg, final Throwable thrown) {
      if (level.compareTo(Level.WARNING) >= 0) {
        final String text = bundle != null ? bundle.getString(msg) : msg;
        Logger.log(fromJavaLevel(level), thrown, text, null);
      }
    }

    @Override
    public void log(final Level level, final ResourceBundle bundle, final String format, final Object... params) {
      if (level.compareTo(Level.WARNING) >= 0) {
        final String text = bundle != null ? bundle.getString(format) : format;
        Logger.log(fromJavaLevel(level), null, MessageFormat.format(text, params), null);
      }
    }
  }

  public static final class SystemLoggerFinder extends System.LoggerFinder {
    @Override
    public System.Logger getLogger(final String name, final Module module) {
      return new SystemLogger();
    }
  }
   */
}
