package tech.dnaco.storage.net;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.gullivernet.commons.util.ScheduledTask;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.arrays.ByteArray;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.RowKeyUtil;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.CounterMap;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;

public class EntityStorageScheduled extends ScheduledTask {
  private static final CounterMap spaceUsed = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("entity_storage_space_used")
    .setLabel("Entity Storage Space Used")
    .register(new CounterMap());

	@Override
	protected void execute() {
    final String[] projectIds = new File("STORAGE_DATA").list();
    for (int i = 0; i < projectIds.length; ++i) {
      Logger.setSession(LoggerSession.newSession(projectIds[i], Logger.getSession()));
      try {
        processProject(projectIds[i]);
      } catch (final Exception e) {
        Logger.error(e, "failed to process {}", projectIds[i]);
      } finally {
        Logger.stopSession();
      }
    }
	}

  private static void processProject(final String projectId) throws Exception {
    final ByteArray prevKey = new ByteArray(64);
    final AtomicLong totalSize = new AtomicLong(0);
    final HashMap<String, TableStats> tables = new HashMap<>();
    Storage.getInstance(projectId).scanAll((key, val) -> {
      final List<byte[]> keyParts = RowKeyUtil.decodeKey(key.buffer());
      totalSize.addAndGet(key.length() + val.length);

      final String group = new String(keyParts.get(0));
      if (group.startsWith(EntitySchema.SYS_TXN_PREFIX)) {
        // no-op
      } else {
        final String table = new String(keyParts.get(1));
        final String column = new String(keyParts.get(keyParts.size() - 1));

        final ByteArray curKey = new ByteArray(64);
        for (int i = 1, n = keyParts.size() - 1; i < n; ++i) {
          curKey.add(keyParts.get(i));
        }

        final TableStats stats = tables.computeIfAbsent(table, TableStats::new);
        stats.groups.add(group);
        stats.add(key, column, val);
        if (!curKey.equals(prevKey)) {
          stats.addRow(curKey);
          prevKey.reset();
          prevKey.add(curKey.rawBuffer(), 0, curKey.size());
        }
      }
    });

    tableStats.put(projectId, tables);

    final StringBuilder builder = new StringBuilder();
    for (final Entry<String, TableStats> entry: tables.entrySet()) {
      entry.getValue().dump(builder);
    }
    Logger.debug("{} total size {}: {}", projectId, HumansUtil.humanSize(totalSize.get()), builder);
    spaceUsed.set(projectId, totalSize.get());
  }

  private static ConcurrentHashMap<String, Map<String, TableStats>> tableStats = new ConcurrentHashMap<>();
  public static TableStats getTableStats(final String tenantId, final String entityName) {
    final Map<String, TableStats> tables = tableStats.get(tenantId);
    if (tables == null) return new TableStats(entityName);

    final TableStats table = tables.get(entityName);
    if (table != null) return table;

    return new TableStats(entityName);
  }

  public static final class TableStats {
    private final Histogram rowKeysHisto = new Histogram(Histogram.DEFAULT_SMALL_SIZE_BOUNDS);
    private final Histogram rowValsHisto = new Histogram(Histogram.DEFAULT_SMALL_SIZE_BOUNDS);
    private final HashSet<String> groups = new HashSet<>();
    private final HashMap<String, Long> fieldSize = new HashMap<>();
    private final String name;
    private long totalSize;
    private long rowCount;

    private TableStats(final String name) {
      this.name = name;
    }

    private void addRow(final ByteArray curKey) {
      rowCount++;
    }

    private void add(final ByteArraySlice key, final String field, final byte[] val) {
      rowKeysHisto.add(key.length());
      rowValsHisto.add(val.length);
      fieldSize.put(field, fieldSize.getOrDefault(field, 0L) + val.length);
      totalSize += key.length() + val.length;
    }

    public long getRowCount() {
      return rowCount;
    }

    public long getDiskUsage() {
      return totalSize;
    }

    public Set<String> getGroups() {
      return groups;
    }

    public long getDiskUsage(final String field) {
      return fieldSize.getOrDefault(field, 0L);
    }

    public void dump(final StringBuilder builder) {
      builder.append("\n---------- Table ").append(name).append(" ----------");
      builder.append("\nRows: ").append(HumansUtil.humanCount(rowCount));
      builder.append("\nSize: ").append(HumansUtil.humanSize(totalSize));
      builder.append("\nGroups: ").append(groups);

      builder.append("\nKeys: ").append(rowKeysHisto.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_SIZE));
      builder.append("\nVals: ").append(rowValsHisto.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_SIZE));
    }
  }

  public static void main(final String[] args) throws Exception {
    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    final String[] projectIds = new File("STORAGE_DATA").list();
    for (int i = 0; i < projectIds.length; ++i) {
      processProject(projectIds[i]);
    }
  }
}
