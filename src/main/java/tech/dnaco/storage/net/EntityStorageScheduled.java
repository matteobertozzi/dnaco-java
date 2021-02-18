package tech.dnaco.storage.net;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.gullivernet.commons.util.ScheduledTask;

import tech.dnaco.bytes.ByteArraySlice;
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
      try {
        processProject(projectIds[i]);
      } catch (final Exception e) {
        Logger.error(e, "failed to process {}", projectIds[i]);
      }
    }
	}

  private static void processProject(final String projectId) throws Exception {
    Logger.setSession(LoggerSession.newSession(projectId, Logger.getSession()));

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
        final TableStats stats = tables.computeIfAbsent(table, TableStats::new);
        stats.groups.add(group);
        stats.add(key, val);
      }
    });

    final StringBuilder builder = new StringBuilder();
    for (final Entry<String, TableStats> entry: tables.entrySet()) {
      entry.getValue().dump(builder);
    }
    Logger.debug("{} total size {}: {}", projectId, HumansUtil.humanSize(totalSize.get()), builder);
    spaceUsed.set(projectId, totalSize.get());
  }

  private static final class TableStats {
    private final Histogram rowKeysHisto = new Histogram(Histogram.DEFAULT_SMALL_SIZE_BOUNDS);
    private final Histogram rowValsHisto = new Histogram(Histogram.DEFAULT_SMALL_SIZE_BOUNDS);
    private final HashSet<String> groups = new HashSet<>();
    private final String name;
    private long totalSize;

    public TableStats(final String name) {
      this.name = name;
    }

    public void add(final ByteArraySlice key, final byte[] val) {
      rowKeysHisto.add(key.length());
      rowValsHisto.add(val.length);
      totalSize += key.length() + val.length;
    }

    public void dump(final StringBuilder builder) {
      builder.append("\n---------- Table ").append(name).append(" ----------");
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
