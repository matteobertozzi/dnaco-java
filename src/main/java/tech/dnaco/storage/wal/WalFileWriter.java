package tech.dnaco.storage.wal;

import java.io.File;
import java.io.FileOutputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import tech.dnaco.journal.JournalBuffer;
import tech.dnaco.journal.JournalWriter;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.StorageConfig;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorGroup;
import tech.dnaco.telemetry.TimeRangeCounter;
import tech.dnaco.telemetry.TimeRangeGauge;

public class WalFileWriter implements JournalWriter {
  private static final DateTimeFormatter ROLL_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS");

  private static final int ROLL_SIZE = 32 << 20;

  private static final class WalFileWriterStats extends TelemetryCollectorGroup {
    private final TimeRangeCounter diskFlushes = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("wal_disk_flushes")
      .setLabel("WAL disk flushes")
      .register(this, new TimeRangeGauge(60, 1, TimeUnit.HOURS));

    private final TimeRangeCounter diskFlushFailures = new TelemetryCollector.Builder()
        .setUnit(HumansUtil.HUMAN_COUNT)
        .setName("wal_disk_failed_flushes")
        .setLabel("WAL disk failed flushes")
        .register(this, new TimeRangeGauge(60, 1, TimeUnit.HOURS));

    private final Histogram diskFlushSizeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("wal_disk_flush_size_histo")
      .setLabel("WAL disk flush size")
      .register(this, new Histogram(Histogram.DEFAULT_SIZE_BOUNDS));

    private final Histogram diskFlushTimeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_TIME_NANOS)
      .setName("wal_disk_flush_time_histo")
      .setLabel("WAL disk flush time")
      .register(this, new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

    public void addFlush(final boolean success, final long flushSize, final long elapsedNs) {
      if (success) {
        diskFlushes.inc();
      } else {
        diskFlushFailures.inc();
      }
      diskFlushSizeHisto.add(flushSize);
      diskFlushTimeHisto.add(elapsedNs);
    }
  }

  private final WalFileWriterStats stats = new TelemetryCollector.Builder()
      .setName("wal_writer")
      .setLabel("WAL Writer")
      .register(new WalFileWriterStats());

  @Override
  public void manageOldLogs() {
    // no-op
  }

  @Override
  public void writeBuffers(final String tenantId, final List<JournalBuffer> buffers) {
    final File logDayDir = new File(StorageConfig.INSTANCE.getWalStorageDir(tenantId), "latest");
    final File logFile = new File(logDayDir, tenantId);
    logFile.getParentFile().mkdirs();

    if (logFile.length() > ROLL_SIZE) {
      final String rollNameName = ROLL_DATE_FORMAT.format(ZonedDateTime.now());
      logFile.renameTo(new File(logFile.getParentFile(), rollNameName));
    }

    boolean flushDone = false;
    do {
      long flushSize = 0;
      final long startTimeNs = System.nanoTime();
      try (FileOutputStream fileStream = new FileOutputStream(logFile, true)) {
        final long startFileSize = fileStream.getChannel().position();

        try (WalEntryWriter writer = new WalEntryWriter()) {
          for (final JournalBuffer threadBuf: buffers) {
            threadBuf.process(tenantId, (buf, off) -> writer.add(fileStream, buf, off));
          }
          writer.flush(fileStream);
        }

        fileStream.flush();
        flushSize += fileStream.getChannel().position() - startFileSize;
        flushDone = true;
      } catch (final Throwable e) {
        Logger.error(e, "unable to flush logs for tenant {}", tenantId);
        flushDone = false;
      } finally {
        stats.addFlush(flushDone, flushSize, System.nanoTime() - startTimeNs);
      }
    } while (!flushDone);
  }
}
