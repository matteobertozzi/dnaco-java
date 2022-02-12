package tech.dnaco.net.logsync;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import tech.dnaco.net.logsync.LogFileUtil.LogsEventListener;
import tech.dnaco.net.logsync.LogFileUtil.LogsStorage;

public class LogsTrackerManager {
  private final CopyOnWriteArrayList<LogsEventListener> logsEventListeners = new CopyOnWriteArrayList<>();
  private final ConcurrentHashMap<String, LogsFileTracker> trackers = new ConcurrentHashMap<>();
  private final LogsStorage storage;

  public LogsTrackerManager(final LogsStorage storage) {
    this.storage = storage;
  }

  public Set<String> getLogsIds() {
    return storage.getLogsIds();
  }

  public LogsTrackerManager registerLogsEventListener(final LogsEventListener listener) {
    this.logsEventListeners.add(listener);
    return this;
  }

  public LogsFileTracker get(final String logsId) {
    LogsFileTracker tracker = trackers.get(logsId);
    if (tracker != null) return tracker;

    synchronized (this) {
      tracker = trackers.get(logsId);
      if (tracker != null) return tracker;

      tracker = newTracker(logsId);
      trackers.put(logsId, tracker);
    }

    for (final LogsEventListener listener: logsEventListeners) {
      listener.newLogEvent(tracker);
    }
    return tracker;
  }

  public void remove(final String logId) {
    final LogsFileTracker tracker = trackers.get(logId);
    for (final LogsEventListener listener: logsEventListeners) {
      listener.removeLogEvent(tracker);
    }
    trackers.remove(logId, tracker);
  }

  private LogsFileTracker newTracker(final String logsId) {
    final LogsFileTracker tracker = new LogsFileTracker(storage.getLogsDir(logsId));
    tracker.loadFiles();
    return tracker;
  }
}
