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
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.StringFormat;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;
import tech.dnaco.tracing.Tracer;

public final class Logger {
  public static final Set<String> EXCLUDE_CLASSES = ConcurrentHashMap.newKeySet();
  static {
    //EXCLUDE_CLASSES.add(Logger.class.getName());

    // log uncaught exceptions
    final UncaughtExceptionHandler ueh = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(new LoggerDefaultUncaughtExceptionHandler(ueh));
  }

  private static final PrintStream STDERR = System.err;
  private static final PrintStream STDOUT = System.out;

  private static LoggingProvider provider = StdoutLogProvider.INSTANCE;
  private static LogLevel defaultLevel = LogLevel.TRACE;

  private Logger() {
    // no-op
  }

  // ===============================================================================================
  //  Provider related
  // ===============================================================================================
  public static void setProvider(final LoggingProvider provider) {
    Logger.provider = provider;
  }

  @SuppressWarnings("unchecked")
  public static <T extends LoggingProvider> T getProvider() {
    return (T) provider;
  }

  // ===============================================================================================
  //  Log Level related
  // ===============================================================================================
  public static LogLevel getDefaultLevel() {
    return defaultLevel;
  }

  public static void setDefaultLevel(final LogLevel level) {
    Logger.defaultLevel = level;
  }

  public static boolean isEnabled(final LogLevel level) {
    //final LoggerSession session = getSession();
    //final LogLevel scopeLevel = (session != null) ? session.getLevel() : defaultLevel;
    final LogLevel scopeLevel = LogLevel.TRACE;
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

  public static void add(final LogEntry entry) {
    provider.addToLog(entry);
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
    // skipFrames = 4 -> [0: lookupLineClassAndMethod(), 1: logRaw(), 2: log(level, ...), 3: info(), 4:userFunc()]
    final String method = LogUtil.lookupLineClassAndMethod(4);

    final TraceId traceId = Tracer.getCurrentTraceId();
    final SpanId spanId = Tracer.getCurrentSpanId();

    final LogEntryMessage entry = new LogEntryMessage();
    add(entry);

    STDOUT.printf("[%s:%s] %s %s: %s%n",
        traceId, spanId, level, method,
        args != null ? StringFormat.format(format, (Object[])args) : format);
    if (exception != null) exception.printStackTrace(STDOUT);
  }

  // ===============================================================================================
  //  PRIVATE Default stdout log provider
  // ===============================================================================================
  private static final class StdoutLogProvider implements LoggingProvider {
    private static final StdoutLogProvider INSTANCE = new StdoutLogProvider();

    private StdoutLogProvider() {
      // no-op
    }

    @Override
    public void addToLog(final LogEntry entry) {
      STDOUT.println(entry.humanReport(new StringBuilder(160)));
    }
  }

  // ===============================================================================================
  //  PRIVATE Default Uncaught Exception Handler
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
