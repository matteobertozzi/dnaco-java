package tech.dnaco.storage;

import java.io.File;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gullivernet.commons.util.JsonConfig;

import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.StringUtil;

public class StorageConfig extends JsonConfig {
  public static final StorageConfig INSTANCE = new StorageConfig();

  public final String SERVICE_INSTANCE_ID = UUID.randomUUID().toString();

  // Service Logger properties
  @JsonProperty("logs.dir") private File logDir;
  @JsonProperty("logs.level") private LogLevel logLevel;
  @JsonProperty("logs.cleaner.interval.days") private int logCleanerIntervalDays;

  // Event Loop properties
  @JsonProperty("eloop.boss.groups") private int eloopBossGroups;
  @JsonProperty("eloop.worker.groups") private int eloopWorkerGroups;

  // Services properties
  @JsonProperty("storage.service.port") private int storageServicePort;
  @JsonProperty("storage.http.port") private int storageHttpPort;

  // Storage properties
  @JsonProperty("storage.dir") private File storageDir;
  @JsonProperty("storage.wal.flush.interval.ms") private int storageWalFlushIntervalMs;


  private StorageConfig() {
    reset();
  }

  public void reset() {
    // Service Logger properties
    this.logDir = null;
    this.logLevel = LogLevel.TRACE;
    this.logCleanerIntervalDays = 7;

    // Event Loop properties
    this.eloopBossGroups = 1;
    this.eloopWorkerGroups = Runtime.getRuntime().availableProcessors();

    // Services properties
    this.storageServicePort = 57327;
    this.storageHttpPort = 57328;

    // Storage properties
    this.storageWalFlushIntervalMs = 500;
  }

  // --------------------------------------------------------------------------------
  //  Service Logger properties
  // --------------------------------------------------------------------------------
  public boolean hasDiskLogger() {
    return (logDir != null) && !StringUtil.equals(logDir.getName(), "stdout");
  }

  public File getLogsDir() {
    return logDir;
  }

  public int getLogCleanerIntervalDays() {
    return logCleanerIntervalDays;
  }

  public LogLevel getLogLevel() {
    return logLevel;
  }

  // --------------------------------------------------------------------------------
  //  Event Loop properties
  // --------------------------------------------------------------------------------
  public int getEloopBossGroups() {
    return eloopBossGroups;
  }

  public int getEloopWorkerGroups() {
    return eloopWorkerGroups;
  }

  // --------------------------------------------------------------------------------
  //  Services properties
  // --------------------------------------------------------------------------------
  public int getStorageServicePort() {
    return storageServicePort;
  }

  public int getStorageHttpPort() {
    return storageHttpPort;
  }

  // --------------------------------------------------------------------------------
  //  Storage properties
  // --------------------------------------------------------------------------------
  public File getStorageDir() {
    return storageDir;
  }

  public File getStorageDir(final String projectId) {
    return new File(getStorageDir(), projectId);
  }

  public File getWalStorageDir(final String projectId) {
    return new File(getStorageDir(projectId), "wal");
  }

  public File getBlocksStorageDir(final String projectId) {
    return new File(getStorageDir(projectId), "blocks");
  }

  public int getStorageWalFlushIntervalMs() {
    return storageWalFlushIntervalMs;
  }
}
