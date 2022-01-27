package tech.dnaco.storage.tools;

import java.io.File;
import java.util.ArrayList;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.iterators.PeekIterator;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.RowKeyUtil;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;

public class DropSchema {
  public static void main(final String[] args) throws Exception {
    final String[] tables = new String[] { "CompanyUser" };
    final String tenantId = "cc-ai-logistics.dev";
    final String[] groups = new String[] { "__ALL__" };

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    for (final String table: tables) {
    if (true) {
      final RocksDbKvStore store = new RocksDbKvStore(tenantId);
        store.openKvStore();

        System.out.println("=========== SCAN TO DELETE ========");
        final ArrayList<ByteArraySlice> toDeleteKeys = new ArrayList<>();
        final ByteArraySlice prefix = RowKeyUtil.newKeyBuilder().add(groups[0]).add(table).addKeySeparator().slice();
        store.scanPrefix(prefix, (k, v) -> {
          final String text = new String(k.buffer());
          System.out.println("ROWS: " + text);
          toDeleteKeys.add(k);
        });

        if (true) {
          for (final ByteArraySlice key: toDeleteKeys) {
            System.out.println("DELETE " + key);
            store.delete(key);
          }
        }

        if (false) {
          for (int i = 0; i < groups.length; ++i) {
            store.deletePrefix(RowKeyUtil.newKeyBuilder()
              .add(groups[i])
              .add("EVENT")
              .addKeySeparator()
              .slice());
          }
        }
        store.closeStorage();
      }

      if (false) {
        final StorageLogic storage = Storage.getInstance(tenantId);
        final EntitySchema schema = storage.getEntitySchema("RS_MAINTENANCE_WORKS");
        final PeekIterator<EntityDataRow> it =  storage.scanRow(null, RowKeyUtil.newKeyBuilder().add("road_safety").add("RS_MAINTENANCE_WORKS").addKeySeparator().slice(), false);
        while (it.hasNext()) {
          System.out.println(it.next());
        }
        return;
      }

      if (true) {
        final StorageLogic storage = Storage.getInstance(tenantId);
        System.out.println(storage.getEntitySchemas());
        final EntitySchema schema = storage.getEntitySchema(table);

        System.out.println(schema);
        storage.dropSchema(table);
        //storage.commit(txn);

        System.out.println(storage.getEntitySchema(table));
      }
    }
  }
}
