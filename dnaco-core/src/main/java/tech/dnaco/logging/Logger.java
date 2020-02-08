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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.function.Supplier;

import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.StringFormat;

// checkout System.logger in java 9
public final class Logger {
  public static final HashSet<String> EXCLUDE_CLASSES = new HashSet<>();
  static {
    EXCLUDE_CLASSES.add(Logger.class.getName());
  }

  private static final ThreadLocal<LoggerSession> localSession = ThreadLocal.withInitial(LoggerSession::newSystemGeneralSession);

  private static final PrintStream STDERR = System.out;
  private static final PrintStream STDOUT = System.err;
  private static LogAsyncWriter writer;
  private static LogLevel defaultLevel = LogLevel.TRACE;

  private Logger() {
    // no-op
  }

  public static void setWriter(final LogAsyncWriter writer) {
    Logger.writer = writer;
  }

  public static LogLevel getDefaultLevel() {
    return defaultLevel;
  }

  public static void setDefaultLevel(final LogLevel level) {
    Logger.defaultLevel = level;
  }

  // ===============================================================================================
  //  Log Session Helpers
  // ===============================================================================================
  public static LoggerSession getSession() {
    return localSession.get();
  }

  public static void setSession(final LoggerSession session) {
    localSession.set(session);
  }

  public static void stopSession() {
    localSession.remove();
  }

  public static long getSessionTraceId() {
    final LoggerSession session = localSession.get();
    return session != null ? session.getTraceId() : -1;
  }

  // ===============================================================================================
  // Log Level Helpers
  // ===============================================================================================
  public static boolean isEnabled(final LogLevel level) {
    final LoggerSession session = getSession();
    LogLevel scopeLevel = (session != null) ? session.getLevel() : defaultLevel;
    return level.ordinal() <= scopeLevel.ordinal();
  }

  public static boolean isTraceEnabled() {
    return isEnabled(LogLevel.TRACE);
  }

  public static boolean isDebugEnabled() {
    return isEnabled(LogLevel.DEBUG);
  }

  public static boolean isInfoEnabled() {
    return isEnabled(LogLevel.INFO);
  }

  // ===============================================================================================
  //  Logging methods
  // ===============================================================================================
  public static void fatal(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.FATAL, exception, format, args);
  }

  public static void fatal(final String format, final Object... args) {
    log(LogLevel.FATAL, null, format, args);
  }

  public static void alert(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.ALERT, exception, format, args);
  }

  public static void alert(final String format, final Object... args) {
    log(LogLevel.ALERT, null, format, args);
  }

  public static void critical(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.CRITICAL, exception, format, args);
  }

  public static void critical(final String format, final Object... args) {
    log(LogLevel.CRITICAL, null, format, args);
  }

  public static void error(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.ERROR, exception, format, args);
  }

  public static void error(final String format, final Object... args) {
    log(LogLevel.ERROR, null, format, args);
  }

  public static void warn(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.WARNING, exception, format, args);
  }

  public static void warn(final String format, final Object... args) {
    log(LogLevel.WARNING, null, format, args);
  }

  public static void warn(final String format, final Supplier<?>... args) {
    log(LogLevel.WARNING, null, format, args);
  }

  public static void info(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.INFO, exception, format, args);
  }

  public static void info(final String format, final Object... args) {
    log(LogLevel.INFO, null, format, args);
  }

  public static void info(final String format, final Supplier<?>... args) {
    log(LogLevel.INFO, null, format, args);
  }

  public static void debug(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.DEBUG, exception, format, args);
  }

  public static void debug(final String format, final Object... args) {
    log(LogLevel.DEBUG, null, format, args);
  }

  public static void debug(final String format, final Supplier<?>... args) {
    log(LogLevel.DEBUG, null, format, args);
  }

  public static void trace(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.TRACE, exception, format, args);
  }

  public static void trace(final String format, final Object... args) {
    log(LogLevel.TRACE, null, format, args);
  }

  public static void trace(final String format, final Supplier<?>... args) {
    log(LogLevel.TRACE, null, format, args);
  }

  public static void raw(final String text) {
    logRaw(LogLevel.ALWAYS, null, text, null);
  }

  // ===============================================================================================
  //  Logging methods
  // ===============================================================================================
  private static void log(final LogLevel level, final Throwable exception, final String format, final Supplier<?>[] args) {
    if (!isEnabled(level)) return;

    final Object[] params;
    if (args == null || args.length == 0) {
      params = null;
    } else {
      params = new Object[args.length];
      for (int i = 0, n = args.length; i < n; ++i) {
        params[i] = args[i].get();
      }
    }

    logRaw(level, exception, format, params);
  }

  private static void log(final LogLevel level, final Throwable exception, final String format, final Object[] args) {
    if (isEnabled(level)) logRaw(level, exception, format, args);
  }

  private static void logRaw(final LogLevel level, final Throwable exception, final String format, final Object[] args) {
    final Thread thread = Thread.currentThread();
    final LoggerSession session = getSession();
    if (session == null) {
      throw new IllegalStateException("expected an active logger session");
    }

    final LogEntry entry = new LogEntry()
        .setModuleId(session.getModuleId())
        .setGroupId(session.getGroupId())
        .setThreadName(thread.getName())
        .setClassAndMethod(buildMethodLine(thread))
        .setStackTrace(exception != null ? stackTraceToString(exception) : null)
        .setTraceId(session.getTraceId())
        .setLevel(level)
        .setTimestamp(System.currentTimeMillis())
        .setFormat(format, args);

    // add to trace buffer for error reporting
    LogTraceBuffer.add(session.getProjectId(), entry);

    // append to the logger
    if (writer != null) {
      writer.addToLogQueue(thread, session.getProjectId(), entry);
    } else {
      entry.printEntry(session.getProjectId(), System.out);
    }
  }

  public static void logToStderr(final LogLevel level, final Throwable throwable,
      final String format, final Object... args) {
    STDERR.println(StringFormat.format(format, args));
    throwable.printStackTrace(System.err);
  }

  public static void logToStderr(final LogLevel level, final String format, final Object... args) {
    STDERR.println(StringFormat.format(format, args));
  }

  public static void logToStdout(final LogLevel level, final String format, final Object... args) {
    STDOUT.println(StringFormat.format(format, args));
  }

  // ===============================================================================================
  //  Trace helper
  // ===============================================================================================
  public static String stackTraceToString(final Throwable exception) {
    final StringWriter writer = new StringWriter();
    try (PrintWriter printWriter = new PrintWriter(writer)) {
      exception.printStackTrace(printWriter);
      printWriter.flush();
      return writer.getBuffer().toString();
    }
  }

  public static String stackTraceToString(final StackTraceElement[] stackTrace) {
    return stackTraceToString(stackTrace, 0);
  }

  public static String stackTraceToString(final StackTraceElement[] stackTrace, final int offset) {
    if (stackTrace == null) return "";

    final StringBuilder builder = new StringBuilder((stackTrace.length - offset) * 32);
    for (int i = offset; i < stackTrace.length; ++i) {
      final StackTraceElement st = stackTrace[i];
      builder.append(getClassName(st)).append('.').append(st.getMethodName()).append("():").append(st.getLineNumber());
      builder.append(System.lineSeparator());
    }
    return builder.toString();
  }

  private static String getClassName(final StackTraceElement st) {
    final String cname = st.getClassName();
    int index = cname.length();
    for (int i = 0; i < 2; i++) {
      final int tmp = cname.lastIndexOf('.', index - 1);
      if (tmp <= 0) break;
      index = tmp;
    }
    return cname.substring(index + 1);
  }

  private static String buildMethodLine(final Thread thread) {
    // Get the stack trace: this is expensive... but really useful
    // NOTE: i should be set to the first public method
    // i = 4 -> [0: getStackTrace(), 1: buildFullTraceMessage(), 2: log(level, ...), 3: info(), 4:userFunc()]
    final StackTraceElement[] stackTrace = thread.getStackTrace();
    for (int i = 3; i < stackTrace.length; ++i) {
      final StackTraceElement st = stackTrace[i];

      // skip helper classes that contains a log indirection
      if (EXCLUDE_CLASSES.contains(st.getClassName())) continue;

      // com.foo.Bar.m1():11
      return getClassName(st) + "." + st.getMethodName() + "():" + st.getLineNumber();
    }
    return null;
  }
}
