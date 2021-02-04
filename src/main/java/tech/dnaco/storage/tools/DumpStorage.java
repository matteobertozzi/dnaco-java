package tech.dnaco.storage.tools;

import java.io.File;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;
import tech.dnaco.storage.net.EntityStorage.CachedScannerResults;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.CounterMap;

public class DumpStorage {
  public static void main(final String[] args) throws Exception {
    final String tenantId = "atg_coldiretti.dev";
    final String[] groups = new String[] { "BACKOFFICE" };

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    final StorageLogic storage = Storage.getInstance(tenantId);
    final StorageLogic storage2 = Storage.getInstance(tenantId + ".2");
    /*
    final Iterator<EntityDataRow> it = storage.getKvStore().scanRow(new ByteArraySlice());
    while (it.hasNext()) {
      final EntityDataRow entity = it.next();
      System.out.println(entity.getSchema().getEntityName() + " " + " -> " + entity.getOperation() + " -> " + new String(entity.buildRowKey()));
    }
    */

    RocksDbKvStore.SKIP_SYS_ROWS = true;
    final Transaction txn = storage.getTransaction(null);

    final CounterMap schemaMap = new CounterMap();
    final CounterMap schemaSize = new CounterMap();
    final CounterMap schemaGroups = new CounterMap();
    final CounterMap schemaOperations = new CounterMap();
    final long startTime = System.nanoTime();
    storage.scanRow(txn, new RowKeyBuilder().slice(), (row) -> {
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
      results.setSchema(schema);

      // scan
      for (final String groupId: groups) {
        storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
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