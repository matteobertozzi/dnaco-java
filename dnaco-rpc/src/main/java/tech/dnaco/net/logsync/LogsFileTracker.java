package tech.dnaco.net.logsync;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;

public class LogsFileTracker {
  private final ArrayList<Consumer<LogsFileTracker>> dataPublishedNotifier = new ArrayList<>();
  private final ArrayList<LogsConsumer> consumers = new ArrayList<>();
  private final ArrayList<String> fileNames = new ArrayList<>();
  private final File logsDir;

  private long maxOffset;
  private long lastPublished;

  public LogsFileTracker(final File logsDir) {
    this.logsDir = logsDir;
    this.maxOffset = 0;
    this.lastPublished = 0;
  }

  public String getLogsId() {
    return logsDir.getName();
  }

  public File getFile(final String name) {
    return new File(logsDir, name);
  }

  protected synchronized List<String> getFileNames() {
    return fileNames;
  }

  public synchronized long getGatingSequence() {
    long minSeq = maxOffset;
    for (final LogsConsumer consumer: this.consumers) {
      minSeq = Math.min(minSeq, consumer.getOffset());
    }
    return minSeq;
  }

  public synchronized List<LogsConsumer> getConsumers() {
    return new ArrayList<>(consumers);
  }

  public synchronized void loadFiles() {
    final String[] files = logsDir.list();
    if (ArrayUtil.isEmpty(files)) {
      Logger.trace("no log file available for dir: {}", logsDir);
      this.fileNames.clear();
      this.maxOffset = 0;
      return;
    }

    // logs name are in the format of <OFFSET>.<not-imporant>
    // so they will get sorted by offset.
    Arrays.sort(files);
    fileNames.clear();
    fileNames.addAll(List.of(files));

    // calculate max offset
    final String lastFileName = files[files.length - 1];
    final File lastFile = new File(logsDir, lastFileName);
    final long lastOffset = LogFileUtil.offsetFromFileName(lastFileName);
    final long lastSize = lastFile.length();
    this.maxOffset = lastOffset + lastSize;
    Logger.debug("found {} logs, {lastOffset} {lastSize} - {maxOffset}", files.length, lastOffset, lastSize, maxOffset);
  }

  public synchronized boolean cleanupFiles(final Duration retainTime) {
    return cleanupFiles(retainTime, getGatingSequence());
  }

  public synchronized boolean cleanupAllFiles(final Duration retainTime, final long gatingSequence) {
    final boolean fullyCleaned = cleanupFiles(retainTime, gatingSequence);
    if (fullyCleaned || fileNames.size() > 1) {
      return fullyCleaned;
    }

    if (gatingSequence < maxOffset) {
      Logger.trace("{} still in use {gatingSeq}/{maxOffset}: {} files active",
        getLogsId(), gatingSequence, fileNames.size());
      return false;
    }

    final String fileName = fileNames.get(0);
    final File lastFile = new File(logsDir, fileName);
    if ((System.currentTimeMillis() - lastFile.lastModified()) < retainTime.toMillis()) {
      Logger.trace("{} last file was last modified {}",
        getLogsId(), HumansUtil.humanTimeMillis(System.currentTimeMillis() - lastFile.lastModified()));
      return false;
    }

    Logger.info("{} {gatingSeq}/{maxOffset} removing {}", getLogsId(), gatingSequence, maxOffset, fileName);
    fileNames.remove(0);
    lastFile.delete();

    Logger.info("{} removing empty dir", getLogsId(), logsDir);
    return logsDir.delete();
  }

  private synchronized boolean cleanupFiles(final Duration retainTime, final long gatingSequence) {
    if (fileNames.isEmpty()) {
      Logger.trace("no log files: {}", logsDir);
      logsDir.delete();
      return true;
    }

    final long retainMs = retainTime.toMillis();
    final long now = System.currentTimeMillis();
    while (fileNames.size() > 1) {
      final String nextFileName = fileNames.get(1);
      final long logOffset = LogFileUtil.offsetFromFileName(nextFileName);
      if (gatingSequence < logOffset) break;

      final String fileName = fileNames.get(0);
      Logger.trace("{} log file {} is candidate for removal {gatingSeq}", getLogsId(), fileName, gatingSequence);
      final long delta = now - LogFileUtil.timestampFromFileName(fileName);
      if (delta < retainMs) {
        Logger.trace("{} log file {} is in the retain period {}: {}",
          getLogsId(), fileName, HumansUtil.humanTimeMillis(retainMs), HumansUtil.humanTimeMillis(delta));
        break;
      }
      Logger.trace("{} {gatingSeq} removing {} created {}",
        getLogsId(), gatingSequence, fileName, HumansUtil.humanTimeMillis(delta));
      new File(logsDir, fileName).delete();
      fileNames.remove(0);
    }

    return false;
  }

  // ==========================================================================================
  //  Publisher
  // ==========================================================================================
  public synchronized long getMaxOffset() {
    return maxOffset;
  }

  public synchronized File getLastBlockFile() {
    if (fileNames.isEmpty()) return null;

    // TODO: PERF: cache?
    return getFile(fileNames.get(fileNames.size() - 1));
  }

  public synchronized File addNewFile() {
    final String name = LogFileUtil.newFileName(maxOffset);
    this.fileNames.add(name);
    for (final LogsConsumer consumer: consumers) {
      consumer.addFile(name);
    }
    return new File(logsDir, name);
  }

  public synchronized void addData(final long length) {
    this.lastPublished = System.currentTimeMillis();
    this.maxOffset += length;
    for (final LogsConsumer consumer: consumers) {
      consumer.addData(length);
    }

    for (final Consumer<LogsFileTracker> consumer: this.dataPublishedNotifier) {
      consumer.accept(this);
    }
  }

  public synchronized void registerDataPublishedListener(final Consumer<LogsFileTracker> consumer) {
    dataPublishedNotifier.add(consumer);
  }

  // ==========================================================================================
  //  Consumer
  // ==========================================================================================
  public synchronized LogsConsumer newConsumer(final String name, final long offset) {
    final LogsConsumer consumer = new LogsConsumer(this, name, offset);
    Logger.debug("{} new consumer {} offset {reqOffset}/{offset}", getLogsId(), name, offset, consumer.getOffset());
    consumers.add(consumer);
    return consumer;
  }

  public synchronized void removeConsumer(final LogsConsumer consumer) {
    consumers.remove(consumer);
  }

  public synchronized void removeAllConsumers() {
    consumers.clear();
  }
}
