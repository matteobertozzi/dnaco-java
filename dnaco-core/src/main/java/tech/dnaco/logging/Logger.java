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

import java.io.PrintStream;
import java.lang.StackWalker.StackFrame;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.function.Supplier;

import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.StringFormat;

public final class Logger {
  public static final HashSet<String> EXCLUDE_CLASSES = new HashSet<>(128);
  static {
    EXCLUDE_CLASSES.add(Logger.class.getName());

    // log uncaught exceptions
    final UncaughtExceptionHandler ueh = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(new LoggerDefaultUncaughtExceptionHandler(ueh));
  }

  private static final PrintStream STDERR = System.err;
  private static final PrintStream STDOUT = System.out;

  private static final ThreadLocal<LoggerSession> localSession = ThreadLocal.withInitial(LoggerSession::newSystemGeneralSession);
  private static LogLevel defaultLevel = LogLevel.TRACE;
  private static JournalAsyncWriter writer = null;

  private Logger() {
    // no-op
  }

  // ===============================================================================================
  //  Logging setup methods
  // ===============================================================================================
  public static void setWriter(final JournalAsyncWriter writer) {
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
    return localSession.get().getTraceId();
  }

  // ===============================================================================================
  // Log Level Helpers
  // ===============================================================================================
  public static boolean isEnabled(final LogLevel level) {
    final LoggerSession session = getSession();
    final LogLevel scopeLevel = (session != null) ? session.getLevel() : defaultLevel;
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
  //  Logging methods (stderr/stdout)
  // ===============================================================================================
  public static void logToStderr(final LogLevel level, final Throwable throwable,
      final String format, final Object... args) {
    STDERR.println(buildTextMessage(level, format, args));
    if (throwable != null) throwable.printStackTrace(STDERR);
  }

  public static void logToStderr(final LogLevel level, final Throwable throwable,
      final String format, final String[] args) {
    STDERR.println(buildTextMessage(level, format, args));
    if (throwable != null) throwable.printStackTrace(STDERR);
  }

  public static void logToStderr(final LogLevel level, final String format, final Object... args) {
    STDERR.println(buildTextMessage(level, format, args));
  }

  public static void logToStdout(final LogLevel level, final String format, final Object... args) {
    STDOUT.println(buildTextMessage(level, format, args));
  }

  private static String buildTextMessage(final LogLevel level, final String format, final Object[] args) {
    return LogEntry.LOG_DATE_FORMAT.format(ZonedDateTime.now())
      + " " +  level + " " + StringFormat.format(format, args);
  }

  // ===============================================================================================
  //  Logging methods
  // ===============================================================================================
  public static void always(final Throwable exception, final String format, final Object... args) {
    log(LogLevel.ALWAYS, exception, format, args);
  }

  public static void raw(final String format, final Object... args) {
    log(LogLevel.ALWAYS, null, format, args);
  }

  public static void always(final String format, final Object... args) {
    log(LogLevel.ALWAYS, null, format, args);
  }

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

  // ===============================================================================================
  //  PRIVATE Logging methods
  // ===============================================================================================
  protected static void log(final LogLevel level, final Throwable exception, final String format, final Supplier<?>[] args) {
    if (!isEnabled(level)) return;

    final String[] params;
    if (args == null || args.length == 0) {
      params = null;
    } else {
      params = new String[args.length];
      for (int i = 0; i < args.length; ++i) {
        final Object obj = args[i].get();
        params[i] = String.valueOf(obj);
      }
    }

    logRaw(level, exception, format, params);
  }

  protected static void log(final LogLevel level, final Throwable exception, final String format, final Object[] args) {
    if (!isEnabled(level)) return;

    final String[] params;
    if (args == null || args.length == 0) {
      params = null;
    } else {
      params = new String[args.length];
      for (int i = 0; i < args.length; ++i) {
        params[i] = StringFormat.valueOf(args[i]);
      }
    }

    logRaw(level, exception, format, params);
  }

  private static void logRaw(final LogLevel level, final Throwable exception,
      final String format, final String[] args) {
    final Thread thread = Thread.currentThread();
    final LogEntryMessage entry = new LogEntryMessage();

    // entry header fields
    final LoggerSession session = getSession();
    entry.setTenantId(session.getTenantId());
    entry.setModule(session.getModuleId());
    entry.setOwner(session.getOwnerId());
    entry.setThread(thread.getName());
    entry.setTraceId(session.getTraceId());
    entry.setTimestamp(System.currentTimeMillis());

    // entry message fields
    entry.setLevel(level);
    entry.setClassAndMethod(lookupLogLineClassAndMethod());
    entry.setMsgFormat(format);
    entry.setMsgArgs(args);
    entry.setException(exception != null ? LogUtil.stackTraceToString(exception) : null);

    // add to trace buffer for error reporting
    LogTraceBuffer.INSTANCE.addToLogQueue(thread, entry);

    // add to writer queue
    add(thread, entry);
  }

  public static void add(final Thread thread, final LogEntry entry) {
    if (writer != null) {
      writer.addToLogQueue(thread, entry);
    } else {
      addToStdout(thread, entry);
    }
  }

  private static void addToStdout(final Thread thread, final LogEntry entry) {
    if (entry instanceof LogEntryTask) {
      final LoggerSession session = getSession();
      if (session != null && session.getLevel().compareTo(LogLevel.INFO) > 0) {
        writeEntryToStdout(thread, entry);
      }
    } else {
      writeEntryToStdout(thread, entry);
    }
  }

  private static void writeEntryToStdout(final Thread thread, final LogEntry entry) {
    STDOUT.println(entry.toHumanReport(new StringBuilder(160)));
  }

  // ===============================================================================================
  //  PRIVATE lookup log line details
  // ===============================================================================================
  private static String lookupLogLineClassAndMethod() {
    // Get the stack trace: this is expensive... but really useful
    // NOTE: i should be set to the first public method
    // i = 4 -> [0: lookupLogLineClassAndMethod(), 1: logRaw(), 2: log(level, ...), 3: info(), 4:userFunc()]
    final StackFrame frame = StackWalker.getInstance().walk(s ->
      s.skip(4)
      .filter(x -> !EXCLUDE_CLASSES.contains(x.getClassName()))
      .findFirst()
    ).get();

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

  public static String stackTraceToString(final Throwable exception) {
    return LogUtil.stackTraceToString(exception);
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

  // ===============================================================================================
  // PRIVATE Default Uncaught Exception Handler
  // ===============================================================================================
  private static final class LoggerDefaultUncaughtExceptionHandler implements UncaughtExceptionHandler {
    private final UncaughtExceptionHandler ueh;

    private LoggerDefaultUncaughtExceptionHandler(final UncaughtExceptionHandler ueh) {
      this.ueh = ueh;
    }

    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      Logger.critical(e, "Uncaught Exception was thrown by thread {}", t.getName());
      if (ueh != null) ueh.uncaughtException(t, e);
    }
  }
}
