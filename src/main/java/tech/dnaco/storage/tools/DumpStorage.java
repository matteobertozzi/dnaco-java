package tech.dnaco.storage.tools;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;
import tech.dnaco.storage.net.CachedScannerResults;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.telemetry.CounterMap;
import tech.dnaco.telemetry.Histogram;

public class DumpStorage {
  public static void main(final String[] args) throws Exception {
    final String tenantId = "road_safety.dev";
    final String[] groups = new String[] { "__ALL__" };

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    final StorageLogic storage = Storage.getInstance(tenantId);
    for (final EntitySchema entry: storage.getEntitySchemas()) {
      System.out.println(entry);
    }
    //if (true) return;

    final long scanTime = System.nanoTime();
    final Histogram histo = new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS);
    System.out.println("===================== SCAN ALL ===============");
    final HashSet<String> streetCodes = new HashSet<>();
    try {
      final Iterator<EntityDataRow> it = storage.getKvStore().scanRow(new ByteArraySlice("__ALL__".getBytes()));
      for (int i = 0; it.hasNext(); ++i) {
        final EntityDataRow entity = it.next();
        if (entity.getOperation() == Operation.DELETE) continue;

        final long st = System.nanoTime();
        final EntityDataRow row = storage.getRow(null, entity);
        if (!StringUtil.equals(Objects.toString(row), entity.toString())) {
          System.out.println("ROW-A: " + row);
          System.out.println("ROW-B: " + entity);
          throw new Exception("mismatch " + i);
        }
        histo.add(System.nanoTime() - st);
        //if ((System.nanoTime() - scanTime) > TimeUnit.SECONDS.toNanos(5)) break;
      }
    } catch (final Throwable e) {
      e.printStackTrace();
    }
    System.out.println(histo.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_TIME_NANOS));
    if (true) return;


    RocksDbKvStore.SKIP_SYS_ROWS = true;
    final Transaction txn = storage.getTransaction(null);

    final CounterMap schemaMap = new CounterMap();
    final CounterMap schemaSize = new CounterMap();
    final CounterMap schemaGroups = new CounterMap();
    final CounterMap schemaOperations = new CounterMap();
    final long startTime = System.nanoTime();
    storage.scanRow(txn, new RowKeyBuilder().slice(), false, (row) -> {
      System.out.println("ROW: " + row);
      schemaMap.inc(row.getSchema().getEntityName());
      schemaSize.inc(row.getSchema().getEntityName(), row.size());
      schemaGroups.inc((String)row.getObject(EntitySchema.SYS_FIELD_GROUP));
      schemaOperations.inc(row.getOperation().name() + " - " + row.getSchema().getEntityName());
      return true;
    });
    final long elapsed = System.nanoTime() - startTime;
    System.out.println(HumansUtil.humanTimeNanos(elapsed));
    System.out.println(schemaMap.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_COUNT));
    System.out.println(schemaSize.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_SIZE));
    System.out.println(schemaGroups.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_COUNT));
    System.out.println(schemaOperations.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_COUNT));
  }

  private static ByteArraySlice rowKeyEntityGroup(final EntitySchema schema, final String groupId) {
    return new RowKeyBuilder().add(groupId).add(schema.getEntityName()).addKeySeparator().slice();
  }

  private void test(final StorageLogic storage) throws Exception {
    final String entity = "ATG_CUSTOMERS";
    final String[] groups = new String[] { "BACKOFFICE" };

      final EntitySchema schema = storage.getOrCreateEntitySchema(entity);
      if (schema == null) throw new UnsupportedOperationException();

      final Transaction txn = storage.getTransaction(null);
      final CachedScannerResults results = new CachedScannerResults();
      results.setSchema(schema, null);

      // scan
      for (final String groupId: groups) {
        storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), false, (row) -> {
          results.add(row);
          System.out.println(row);
          return true;
        });
      }
      results.sealResults();
  }
}

/*
- 41.98 (     68) - ATG_SENSORI
- 20.37 (     33) - ATG_USERS
- 16.05 (     26) - ATG_KML
-  7.41 (     12) - ATG_CAMPI
-  6.17 (     10) - ATG_CUSTOMERS
-  4.32 (      7) - ATG_TOKEN
-  3.09 (      5) - ATG_SOILS_GROUP
-  0.62 (      1) - ATG_SENSORS_TEMPLATE
*/