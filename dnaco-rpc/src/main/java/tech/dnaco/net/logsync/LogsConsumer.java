package tech.dnaco.net.logsync;

import java.io.File;
import java.util.ArrayList;
import java.util.function.Consumer;

import tech.dnaco.logging.Logger;

public class LogsConsumer {
  private final ArrayList<String> fileNames = new ArrayList<>();
  private final LogsFileTracker tracker;
  private final String name;

  private long offset;
  private long blkOffset;
  private long nextOffset;
  private long maxOffset;

  public LogsConsumer(final LogsFileTracker tracker, final String name, final long offset) {
    this.tracker = tracker;
    this.name = name;
    this.setOffset(offset);
  }

  public String getName() {
    return name;
  }

  public String getLogsId() {
    return tracker.getLogsId();
  }

  protected ArrayList<String> getFileNames() {
    return new ArrayList<>(fileNames);
  }

  protected File getFile(final String name) {
    return tracker.getFile(name);
  }

  public synchronized void setOffset(final long newOffset) {
    this.offset = 0;
    this.blkOffset = 0;
    this.nextOffset = 0;
    this.maxOffset = tracker.getMaxOffset();

    fileNames.clear();
    fileNames.addAll(tracker.getFileNames());
    Logger.debug("{} try setting {newOffset} {maxOffset} - {} files", getLogsId(), newOffset, maxOffset, fileNames.size());
    if (fileNames.isEmpty()) return;

    if (newOffset > maxOffset) {
      Logger.fatal("{} invalid offset {newOffset} {maxOffset} - {}", getLogsId(), newOffset, maxOffset, fileNames);
      throw new IllegalArgumentException(getLogsId() + " invalid offset " + newOffset + ", max offset is " + maxOffset);
    }

    int fileCount = fileNames.size();
    while (fileCount > 1) {
      final long nextOffset = LogFileUtil.offsetFromFileName(fileNames.get(1));
      if (nextOffset > newOffset) break;

      fileNames.remove(0);
      fileCount--;
    }

    if (fileCount > 0) {
      final long firstOffset = LogFileUtil.offsetFromFileName(fileNames.get(0));
      if (newOffset < firstOffset) {
        Logger.fatal("{} invalid offset {newOffset} {firstOffset} - {}", getLogsId(), newOffset, firstOffset, fileNames);
        throw new IllegalArgumentException(getLogsId() + " invalid offset " + newOffset + ", first log available is " + firstOffset);
      }

      // calculate offset & next offset
      this.offset = newOffset;
      this.blkOffset = newOffset - firstOffset;
      this.nextOffset = (fileNames.size() > 1) ? LogFileUtil.offsetFromFileName(fileNames.get(1)) : maxOffset;
      Logger.info("{} new offset set to {}/{} {blkOffset}, files {}", getLogsId(), offset, nextOffset, blkOffset, fileNames);
      return;
    }

    throw new UnsupportedOperationException("unexpected offset " + newOffset + " files " + fileNames);
  }

  public synchronized boolean hasMore() {
    if (getBlockAvailable() > 0) return true;
    if (fileNames.size() <= 1) return false;

    // grab the next log
    nextLog();
    return getBlockAvailable() > 0;
  }

  public synchronized long getMaxOffset() {
    return maxOffset;
  }

  public synchronized long getOffset() {
    return offset;
  }

  public synchronized long getBlockOffset() {
    return blkOffset;
  }

  public synchronized long getBlockAvailable() {
    return nextOffset - offset;
  }

  public synchronized File getBlockFile() {
    // TODO: PERF: cache?
    return tracker.getFile(fileNames.get(0));
  }

  public synchronized void consume(long consumed) {
    if (consumed <= 0) {
      Logger.debug("{} nothing consumed: {blkAvail} {offset}/{maxOffset} - {files}",
        getLogsId(), getBlockAvailable(), offset, maxOffset, fileNames);
      return;
    }

    while (consumed > 0) {
      final long length = Math.min(consumed, getBlockAvailable());
      if (length <= 0) {
        Logger.debug("{} CONSUMED SOMETHING WRONG: {consumedLength}: {blkAvail} {offset}/{maxOffset} - {files}",
          getLogsId(), consumed, getBlockAvailable(), offset, maxOffset, fileNames);
        throw new UnsupportedOperationException();
      }

      consumed -= length;
      this.offset += length;
      this.blkOffset += length;

      final long blkAvail = getBlockAvailable();
      //Logger.trace("{} consume {}. avail:{} offset:{} blkOffset:{}", getLogsId(), length, blkAvail, offset, blkOffset);
      if (blkAvail > 0) return;

      // nothing more available
      if (offset == maxOffset) {
        Logger.trace("{} log {} fully consumed. current offset {}/{}", getLogsId(), fileNames.get(0), offset, maxOffset);
        return;
      }

      if (fileNames.size() == 1) {
        Logger.fatal("TRYING TO GRAB A NON EXISTENT NEXT LOG {}: {offset} {maxOffset}: {blkAvail} {blkOff}: {nextOffset} {files}",
          getLogsId(), offset, maxOffset, blkAvail, blkOffset, nextOffset, fileNames);
      }

      // grab the next log
      nextLog();
    }
  }

  private void nextLog() {
    // grab the next log
    final String fileName = fileNames.remove(0);
    Logger.trace("{} log {} fully consumed. current offset {}/{}", getLogsId(), fileName, offset, maxOffset);

    // check the next file and the current offset
    final String nextFile = fileNames.get(0);
    if (LogFileUtil.offsetFromFileName(nextFile) != offset) {
      throw new IllegalStateException(getLogsId() + " expected log starting from offset " + offset + ", got " + nextFile);
    }

    // calculate next offset
    this.blkOffset = 0;
    this.nextOffset = (fileNames.size() > 1) ? LogFileUtil.offsetFromFileName(fileNames.get(1)) : maxOffset;
    Logger.info("{} {currentFile} {offset} {nextOffset}", getLogsId(), nextFile, offset, nextOffset);
  }

  // ==========================================================================================
  //  Publisher methods
  // ==========================================================================================
  protected synchronized void addFile(final String name) {
    this.fileNames.add(name);
  }

  protected synchronized void addData(final long length) {
    if (fileNames.size() == 1) {
      // the data is being appended to the current file
      this.nextOffset += length;
    }
    this.maxOffset += length;
  }

  public void registerDataPublishedListener(final Consumer<LogsFileTracker> consumer) {
    tracker.registerDataPublishedListener(consumer);
  }
}
