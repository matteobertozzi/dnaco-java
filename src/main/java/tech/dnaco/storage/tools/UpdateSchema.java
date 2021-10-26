package tech.dnaco.storage.tools;

import java.io.File;

import tech.dnaco.storage.demo.EntityDataType;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;

public class UpdateSchema {
  public static void main(final String[] args) throws Exception {
    final String tenantId = "road_safety.dev";

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);
    final StorageLogic storage = Storage.getInstance(tenantId);

    final EntitySchema schema = storage.getEntitySchema("RS_MAINTENANCE_WORKS");
    schema.update("dateEnd", EntityDataType.STRING);
    schema.update("timeEnd", EntityDataType.STRING);
    storage.registerSchema(schema);
  }
}
