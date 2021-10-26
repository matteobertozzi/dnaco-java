package tech.dnaco.storage.tools;

import java.io.File;
import java.nio.charset.StandardCharsets;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.RowKeyUtil;
import tech.dnaco.collections.iterators.PeekIterator;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.JvmMetrics;
import tech.dnaco.threading.ThreadUtil;

public class PerfScanTool {
  public static void main(final String[] args) throws Exception {
    final String tenantId = "road_safety.dev";
    final String[] groups = new String[] { "__ALL__" };

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    final StorageLogic storage = Storage.getInstance(tenantId);
    final ByteArraySlice key = RowKeyUtil.newKeyBuilder().add(groups[0]).add("RS_DISTRICTS").slice();
    ThreadUtil.runInThreads("foo", 32, () -> {
      final long startTime = System.nanoTime();
      long x = 0;
      try {
        final PeekIterator<EntityDataRow> it = storage.scanRow(null, key, true);
        while (it.hasNext()) {
          final EntityDataRow entity = it.next();
          x += entity.size();
        }
      } catch (final Exception e) {
        Logger.error(e, "failed");
      }
      final long elapsed = System.nanoTime() - startTime;
      System.out.println(HumansUtil.humanTimeNanos(elapsed)
        + " -> " + HumansUtil.humanSize(JvmMetrics.INSTANCE.getUsedMemory())
        + " -> " + HumansUtil.humanSize(x));
    });
  }

  private static final class EntityDataWriter {
    private final byte[] buffer = new byte[1 << 10];

    public void write(final EntitySchema schema) {
      // | length | entity name |
      final byte[] entityName = schema.getEntityName().getBytes(StandardCharsets.UTF_8);
      writeVarInt(entityName.length);
      write(entityName);

      // | fields count | [ field types ] |
      final int fieldsCount = schema.fieldsCount();
      writeVarInt(fieldsCount);
      for (int i = 0; i < fieldsCount; ++i) {
        write(schema.getFieldType(i).ordinal() & 0xff);
      }
      // | keys | fields |
      writeVarInt(schema.keyFieldsCount());
      writeVarInt(schema.nonKeyFieldsCount());
      // key fields: [ length | field names ]
      for (final String name: schema.getKeyFields()) {
        final byte[] rawName = name.getBytes(StandardCharsets.UTF_8);
        writeVarInt(rawName.length);
        write(rawName);
      }
      // non-key fields: [ length | field names ]
      for (final String name: schema.getNonKeyFields()) {
        final byte[] rawName = name.getBytes(StandardCharsets.UTF_8);
        writeVarInt(rawName.length);
        write(rawName);
      }
    }

    public void write(final EntityDataRow row) {
      final int fieldsCount = row.getSchema().fieldsCount();
      //writeVarInt(row.size());)
      //writeVarInt(value);
    }

    private void writeVarInt(final long value) {}
    private void write(final byte[] value) {}
    private void write(final int value) {}
  }
}
