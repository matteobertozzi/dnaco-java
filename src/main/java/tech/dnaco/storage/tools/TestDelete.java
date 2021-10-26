package tech.dnaco.storage.tools;

import java.io.File;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.UUID;

import com.gullivernet.commons.util.DateUtil;

import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntityDataType;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.CounterMap;

public class TestDelete {
  public static void main(final String[] args) throws Exception {
    final String tenantId = "matteo";
    final String[] groups = new String[] { "TEST" };

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    final EntitySchema schema = new EntitySchema("foo");
    schema.setKey(new String[] { "k1", "k2" });
    schema.update("k1", EntityDataType.STRING);
    schema.update("k2", EntityDataType.STRING);
    schema.update("f1", EntityDataType.INT);
    schema.update("f2", EntityDataType.BYTES);

    final StorageLogic storage = Storage.getInstance(tenantId);
    final Transaction txn = storage.getOrCreateTransaction(UUID.randomUUID().toString());
    if (false) {
      storage.registerSchema(schema);
      final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
      for (int i = 0; i < 100; ++i) {
        final byte[] blob = new byte[1 << 20];
        Arrays.fill(blob, (byte)(i % 0xff));

        final EntityDataRows row = new EntityDataRows(schema, false).newRow();
        row.addObject(EntitySchema.SYS_FIELD_TIMESTAMP, timestamp);
        row.addObject(EntitySchema.SYS_FIELD_GROUP, "TEST");
        row.addObject(EntitySchema.SYS_FIELD_SEQID, Long.MAX_VALUE);
        row.addObject("k1", "k1-" + i);
        row.addObject("k2", "k2-" + i);
        if (false) {
          row.addObject(EntitySchema.SYS_FIELD_OPERATION, Operation.INSERT.ordinal());
          row.addObject("f1", i * 100);
          row.addObject("f2", blob);
        } else {
          row.addObject(EntitySchema.SYS_FIELD_OPERATION, Operation.DELETE.ordinal());
        }
        storage.addRow(txn, new EntityDataRow(row, 0));
      }
      storage.commit(txn);
    }

    RocksDbKvStore.SKIP_SYS_ROWS = true;
    final CounterMap schemaMap = new CounterMap();
    final CounterMap schemaSize = new CounterMap();
    final CounterMap schemaGroups = new CounterMap();
    final CounterMap schemaOperations = new CounterMap();
    final long startTime = System.nanoTime();
    storage.scanRow(storage.getTransaction(null), new RowKeyBuilder().slice(), false, (row) -> {
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
}
