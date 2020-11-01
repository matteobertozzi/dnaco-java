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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.io.FileUtil;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TimeRangeCounter;
import tech.dnaco.telemetry.TimeRangeGauge;

public class LogFileWriter implements LogWriter {
  private static final DateTimeFormatter LOG_FOLDER_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final DateTimeFormatter ROLL_DATE_FORMAT = DateTimeFormatter.ofPattern("HHmmssSSS");
  private static final int ROLL_SIZE = 32 << 20;

  private final TimeRangeCounter diskFlushes = new TelemetryCollector.Builder()
      .setName("logger_disk_flushes")
      .setLabel("Logger disk flushes")
      .setUnit(HumansUtil.HUMAN_COUNT)
      .register(new TimeRangeGauge(60, 1, TimeUnit.HOURS));

  private final Histogram diskFlushSizeHisto = new TelemetryCollector.Builder()
      .setName("logger_disk_flush_size_histo")
      .setLabel("Logger disk flush size")
      .setUnit(HumansUtil.HUMAN_SIZE)
      .register(new Histogram(Histogram.DEFAULT_SIZE_BOUNDS));

  private final Histogram diskFlushTimeHisto = new TelemetryCollector.Builder()
      .setName("logger_disk_flush_time_histo")
      .setLabel("Logger disk flush time")
      .setUnit(HumansUtil.HUMAN_TIME_NANOS)
      .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private final File logDir;
  private final int deleteDays;

  public LogFileWriter(final File logDir, final int deleteDays) {
    this.logDir = logDir;
    this.deleteDays = deleteDays;
  }

  @Override
  public void writeQueue(final String projectId, final List<LogBuffer> buffers) {
    final LocalDateTime now = LocalDateTime.now();

    final File logDayDir = new File(logDir, LOG_FOLDER_DATE_FORMAT.format(now));
    final File logFile = new File(logDayDir, StringUtil.defaultIfEmpty(projectId, "general"));
    logFile.getParentFile().mkdirs();
    // Logger.logToStderr(LogLevel.ALERT, "unable to create log directory: {}", logFile.getParentFile());

    if (logFile.length() > ROLL_SIZE) {
      final String rollNameName = logFile.getName() + "." + ROLL_DATE_FORMAT.format(now);
      logFile.renameTo(new File(logFile.getParentFile(), rollNameName));
    }

    try (OutputStream stream = new GZIPOutputStream(new FileOutputStream(logFile, true))) {
      final long startTime = System.nanoTime();
      long flushSize = 0;
      for (final LogBuffer buf: buffers) {
        flushSize += buf.writeTo(projectId, stream);
      }
      final long elapsed = System.nanoTime() - startTime;
      diskFlushes.inc();
      diskFlushSizeHisto.add(flushSize);
      diskFlushTimeHisto.add(elapsed);
    } catch (final IOException e) {
      Logger.logToStderr(LogLevel.ERROR, e, "unable to flush logs for groupId {}", projectId);
    }
  }

  @Override
  public void manageOldLogs() {
    if (!logDir.exists()) return;

    final File[] dateSubFolders = logDir.listFiles();
    if (ArrayUtil.isEmpty(dateSubFolders)) return;

    final long CLEANUP_MAX_TIME = TimeUnit.SECONDS.toNanos(2);
    final long startTime = System.nanoTime();
    final long eligibleForDeletion = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(deleteDays);
    for (int i = 0; i < dateSubFolders.length; ++i) {
      final File dateSubFolder = dateSubFolders[i];
      try {
        if (!dateSubFolder.isDirectory()) continue;
        if (!isElegibleForDeletion(dateSubFolder, eligibleForDeletion)) continue;

        FileUtil.recursiveDelete(dateSubFolder);
      } catch (final Throwable e) {
        Logger.error(e, "unable to delete old logs: {}", dateSubFolder);
      }

      if ((System.nanoTime() - startTime) > CLEANUP_MAX_TIME) {
        Logger.trace("cleanup is taking too long. running the next time");
        break;
      }
    }
  }

  private static boolean isElegibleForDeletion(final File folder, final long elegibleTs) {
    final String name = folder.getName().substring(0, 10); // yyyy-MM-dd
    final LocalDate folderDate = LocalDate.parse(name, LOG_FOLDER_DATE_FORMAT);
    return TimeUnit.DAYS.toMillis(folderDate.toEpochDay()) < elegibleTs;
  }

  @Override
  public String toString() {
    return "LogFileWriter[" + logDir + "]";
  }
}
