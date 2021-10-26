package tech.dnaco.storage.tools;

import java.io.File;
import java.util.Arrays;

import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;

public class TestWrite {
  public static void main(final String[] args) throws Exception {
    final String tenantId = "road_safety.dev";
    final String[] groups = new String[] { "road_safety" };

    RocksDbKvStore.init(new File("STORAGE_DATA"), 512 << 20);

    final StorageLogic storage = Storage.getInstance(tenantId);
    final EntitySchema schema = storage.getEntitySchema("RS_MAINTENANCE_WORKS");
    System.out.println(Arrays.toString(schema.getKeyFields()));
    System.out.println(schema.getNonKeyFields());

    final Transaction txn = storage.getOrCreateTransaction(null);
    for (int i = 0; i < 10000; ++i) {
      final EntityDataRows rows = new EntityDataRows(schema, false);
      rows.newRow();
      rows.addObject("__seqId__", i);
      rows.addObject("__ts__", System.currentTimeMillis());
      rows.addObject("__op__", 1);
      rows.addObject("__group__", "__ALL__");
      rows.addObject("id", String.format("kay-%07d", i));
      rows.addObject("dateStart", String.format("ds-%07d", i));
      rows.addObject("dateEnd", String.format("ds-%07d", i));
      rows.addObject("refId", String.format("refId-%07d", i));
      rows.addObject("note", String.format("note-%07d", i));
      storage.addRow(txn, new EntityDataRow(rows, 0));
      System.out.println("store row: " + i);
    }
    storage.commit(txn);
  }
}
