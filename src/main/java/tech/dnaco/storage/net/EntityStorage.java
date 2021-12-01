package tech.dnaco.storage.net;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.gullivernet.commons.util.DateUtil;
import com.gullivernet.commons.util.VerifyArg;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntityDataType;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.storage.demo.logic.Query;
import tech.dnaco.storage.demo.logic.Query.QueryCache;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;
import tech.dnaco.storage.net.CachedScannerResults.CachedScanResults;
import tech.dnaco.storage.net.EntityStorageScheduled.TableStats;
import tech.dnaco.storage.net.models.ClientSchema;
import tech.dnaco.storage.net.models.ClientSchema.EntityField;
import tech.dnaco.storage.net.models.CountRequest;
import tech.dnaco.storage.net.models.CountResult;
import tech.dnaco.storage.net.models.JsonEntityDataRows;
import tech.dnaco.storage.net.models.ModificationRequest;
import tech.dnaco.storage.net.models.ModificationWithFilterRequest;
import tech.dnaco.storage.net.models.ScanNextRequest;
import tech.dnaco.storage.net.models.ScanRequest;
import tech.dnaco.storage.net.models.ScanResult;
import tech.dnaco.storage.net.models.Scanner;
import tech.dnaco.storage.net.models.SchemaRequest;
import tech.dnaco.storage.net.models.TransactionCommitRequest;
import tech.dnaco.storage.net.models.TransactionStatusResponse;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.telemetry.ConcurrentMaxAndAvgTimeRangeGauge;
import tech.dnaco.telemetry.ConcurrentTopK;
import tech.dnaco.telemetry.CounterMap;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TopK.TopType;
import tech.dnaco.tracing.Tracer;

public final class EntityStorage {
  public static final EntityStorage INSTANCE = new EntityStorage();

  private final CounterMap opsCount = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("entity_storage_ops_count")
    .setLabel("Entity Storage Ops Count")
    .register(new CounterMap());

  private final ConcurrentMaxAndAvgTimeRangeGauge modifyRows = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("entity_storage_modify_row_count")
    .setLabel("Entity Storage Modify Row Count")
    .register(new ConcurrentMaxAndAvgTimeRangeGauge(24 * 60, 1, TimeUnit.MINUTES));

  private final ConcurrentTopK modifyTopRows = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("entity_storage_modify_top_row_count")
    .setLabel("Entity Storage Modify Top Row Count")
    .register(new ConcurrentTopK(TopType.MIN_MAX, 32));

  public void createEntitySchema(final ClientSchema request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    if (storage.getEntitySchema(request.getName()) != null) {
      throw new Exception("entity named " + request.getName() + " already exists");
    }

    final EntitySchema schema = storage.getOrCreateEntitySchema(request.getName());
    schema.setSync(request.getSync());
    schema.setDataType(request.getDataType());
    schema.setRetentionPeriod(request.getRetentionPeriod());
    schema.setLabel(request.getLabel());

    final ArrayList<String> keys = new ArrayList<>();
    for (final EntityField field: request.getFields()) {
      if (field.isKey()) keys.add(field.getName());

      if (!schema.update(field.getName(), EntityDataType.valueOf(field.getType()))) {
        throw new Exception("Invalid field type: " + field.getName() + " " + field.getType());
      }
    }

    schema.setKey(keys.toArray(new String[0]));

    storage.registerSchema(schema);
  }

  public void editEntitySchema(final ClientSchema request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = storage.getEntitySchema(request.getName());
    if (schema == null) {
      throw new Exception("entity named " + request.getName() + " does not exists");
    }

    schema.setSync(request.getSync());
    schema.setDataType(request.getDataType());
    schema.setRetentionPeriod(request.getRetentionPeriod());
    schema.setLabel(request.getLabel());
    schema.setModificationTime(System.currentTimeMillis());

    if (ArrayUtil.isNotEmpty(request.getFields())) {
      for (final EntityField field: request.getFields()) {
        if (field.isKey() || schema.isKey(field.getName())) {
          continue;
        }

        if (!schema.update(field.getName(), EntityDataType.valueOf(field.getType()))) {
          throw new Exception("Invalid field type: " + field.getName() + " " + field.getType());
        }
      }
    }

    storage.registerSchema(schema);
  }

  public ClientSchema getEntitySchema(final SchemaRequest request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.getTenantId());
    final EntitySchema schema = storage.getEntitySchema(request.getName());
    final TableStats stats = EntityStorageScheduled.getTableStats(request.getTenantId(), request.getName());
    return schema != null ? schema.toClientJson(stats, true) : null;
  }

  public List<ClientSchema> getEntitySchemas(final SchemaRequest request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.getTenantId());
    final Collection<EntitySchema> schemas = storage.getEntitySchemas();
    final ArrayList<ClientSchema> json = new ArrayList<>(schemas.size());
    for (final EntitySchema schema: schemas) {
      final TableStats stats = EntityStorageScheduled.getTableStats(request.getTenantId(), schema.getEntityName());
      json.add(schema.toClientJson(stats, false));
    }
    json.sort((a, b) -> a.getName().compareTo(b.getName()));
    return json;
  }

  // ================================================================================
  //  Modification Handlers
  // ================================================================================
  public TransactionStatusResponse insertEntity(final ModificationRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    return modify(request, Operation.INSERT);
  }

  public TransactionStatusResponse upsertEntity(final ModificationRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    return modify(request, Operation.UPSERT);
  }

  public TransactionStatusResponse updateEntity(final ModificationRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    return modify(request, Operation.UPDATE);
  }

  public TransactionStatusResponse updateEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    verifyGroups(request.getGroups());
    opsCount.inc(request.getTenantId());

    final long startTime = System.nanoTime();

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = updateSchema(storage, request.getEntity(), null, new JsonEntityDataRows[] { request.getFieldsToUpdate() });
    if (schema == null) {
      return new TransactionStatusResponse(Transaction.State.FAILED);
    }

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.getTxnId());

    final QueryCache queryCache = new QueryCache();
    for (final String groupId: request.getGroups()) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), false, (row) -> {
        if (request.hasNoFilter() || Query.process(request.getFilter(), row, queryCache)) {
          final EntityDataRows updatedRow = new EntityDataRows(schema, row.hasAllFields()).newRow();
          updatedRow.copyFrom(row);
          updatedRow.setTimestamp(0, timestamp);
          updatedRow.setOperation(0, Operation.UPDATE);
          storage.addRow(txn, new EntityDataRow(updatedRow, 0));
        }
        return true;
      });
    }

    commitIfLocalTxn(storage, txn);

    final long elapsed = System.nanoTime() - startTime;
    Logger.debug("update {} {} filtered took {}",
      request.getTenantId(), schema.getEntityName(), HumansUtil.humanTimeNanos(elapsed));

    return new TransactionStatusResponse(txn.getState());
  }

  public TransactionStatusResponse deleteEntity(final ModificationRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    return modify(request, Operation.DELETE);
  }

  public TransactionStatusResponse deleteEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    verifyGroups(request.getGroups());
    opsCount.inc(request.getTenantId());

    final long startTime = System.nanoTime();

    final StorageLogic storage = Storage.getInstance(request.getTenantId());
    final EntitySchema schema = storage.getOrCreateEntitySchema(request.getEntity());

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.getTxnId());

    final QueryCache queryCache = new QueryCache();
    for (final String groupId: request.getGroups()) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), false, (row) -> {
        if (request.hasNoFilter() || Query.process(request.getFilter(), row, queryCache)) {
          final EntityDataRows updatedRow = new EntityDataRows(schema, false).newRow();
          updatedRow.copyFrom(row);
          updatedRow.setTimestamp(0, timestamp);
          updatedRow.setOperation(0, Operation.DELETE);
          storage.addRow(txn, new EntityDataRow(updatedRow, 0));
        }
        return true;
      });
    }

    commitIfLocalTxn(storage, txn);

    final long elapsed = System.nanoTime() - startTime;
    Logger.debug("delete {} {} filtered took {}",
      request.getTenantId(), schema.getEntityName(), HumansUtil.humanTimeNanos(elapsed));

    return new TransactionStatusResponse(txn.getState());
  }

  public TransactionStatusResponse commit(final TransactionCommitRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    opsCount.inc(request.getTenantId());

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final Transaction txn = storage.getTransaction(request.getTxnId());
    if (txn == null) throw new IllegalArgumentException("invalid txnId");

    if (request.isRollback()) {
      storage.rollback(txn);
    } else {
      storage.commit(txn);
    }
    return new TransactionStatusResponse(txn.getState());
  }

  private EntitySchema updateSchema(final StorageLogic storage, final String entity,
      final String[] keys, final JsonEntityDataRows[] rows) throws Exception {
    // apply schema checks and modifications
    final EntitySchema schema = storage.getOrCreateEntitySchema(entity);
    if (keys != null && !schema.setKey(keys)) {
      // TODO: schema mismatch
      return null;
    }

    for (final JsonEntityDataRows jsonRows: rows) {
      if (!schema.update(jsonRows.getFieldNames(), jsonRows.getTypes())) {
        // TODO: schema mismatch
        return null;
      }
    }

    // update the schema
    storage.registerSchema(schema);
    return schema;
  }

  private TransactionStatusResponse modify(final ModificationRequest request, final Operation operation) throws Exception {
    final long startTime = System.nanoTime();
    verifyGroups(request.getGroups());
    opsCount.inc(request.getTenantId());

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = updateSchema(storage, request.getEntity(), request.getKeys(), request.getRows());
    if (schema == null) {
      return new TransactionStatusResponse(Transaction.State.FAILED);
    }

    // add to TXN log
    final LongValue rowCount = new LongValue();
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.getTxnId());
    for (final JsonEntityDataRows jsonRows: request.getRows()) {
      if (txn.getState() != Transaction.State.PENDING) break;

      jsonRows.forEachEntityRow(schema, request.getGroups(), (row) -> {
        row.setOperation(operation);
        row.setTimestamp(timestamp);
        if (!storage.addRow(txn, row)) {
          txn.setState(Transaction.State.FAILED);
          return false;
        }
        rowCount.incrementAndGet();
        return true;
      });
    }

    commitIfLocalTxn(storage, txn);

    final long elapsed = System.nanoTime() - startTime;
    Logger.debug("{} {} {} took {} for {} rows",
      operation, request.getTenantId(), schema.getEntityName(),
      HumansUtil.humanTimeNanos(elapsed), rowCount.get());
    modifyRows.update(rowCount.get());
    modifyTopRows.add(request.getTenantId() + " " + operation + " " + schema.getEntityName(), rowCount.get());

    return new TransactionStatusResponse(txn.getState());
  }

  private void commitIfLocalTxn(final StorageLogic storage, final Transaction txn) throws Exception {
    if (txn.isLocal()) {
      Logger.debug("local transaction {} state {}", txn.getTxnId(), txn.getState());
      if (txn.getState() == Transaction.State.PENDING) {
        storage.commit(txn);
      } else {
        storage.rollback(txn);
      }
    }
  }

  // ================================================================================
  //  Get/Scan Handlers
  // ================================================================================
  public Scanner getEntity(final ScanRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    return scanEntity(request);
  }

  public Scanner scanEntity(final ScanRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    verifyGroups(request.getGroups());
    opsCount.inc(request.getTenantId());

    final long startTime = System.nanoTime();

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = storage.getEntitySchema(request.getEntity());
    if (schema == null) return Scanner.EMPTY;

    final Transaction txn = storage.getTransaction(request.getTxnId());
    return buildScanner(request.getTenantId(), request.getEntity(), startTime, (results) -> {
      results.setSchema(schema);

      // direct get calls
      if (ArrayUtil.isNotEmpty(request.getRows())) {
        for (final JsonEntityDataRows jsonRows: request.getRows()) {
          jsonRows.forEachEntityRow(schema, request.getGroups(), (row) -> {
            final EntityDataRow result = storage.getRow(txn, row);
            if (result != null) results.add(result);
            return true;
          });
        }
        return;
      }

      // scan
      final QueryCache queryCache = new QueryCache();
      for (final String groupId: request.getGroups()) {
        storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), request.shouldIncludeDeleted(), (row) -> {
          if (request.hasNoFilter() || Query.process(request.getFilter(), row, queryCache)) {
            results.add(row);
          }
          results.incRowRead();
          return true;
        });
      }
    });
  }

  private final ConcurrentHashMap<String, CachedScanResults> scanResults = new ConcurrentHashMap<>();
  public Scanner scanAll(final ScanRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    verifyGroups(request.getGroups());
    opsCount.inc(request.getTenantId());

    final long startTime = System.nanoTime();
    final boolean syncOnly = request.isSyncOnly();
    final StorageLogic storage = Storage.getInstance(request.getTenantId());
    final Transaction txn = storage.getTransaction(request.getTxnId());

    final QueryCache queryCache = new QueryCache();
    return buildScanner(request.getTenantId(), null, startTime, (results) -> {
      for (final String groupId: request.getGroups()) {
        final ByteArraySlice prefix = new RowKeyBuilder().add(groupId).addKeySeparator().slice();
        storage.scanRow(txn, prefix, request.shouldIncludeDeleted(), (row) -> {
          if (!syncOnly || row.getSchema().getSync()) {
            if (request.hasNoFilter() || Query.process(request.getFilter(), row, queryCache)) {
              results.add(row);
            }
          }
          results.incRowRead();
          return true;
        });
      }
    });
  }

  @FunctionalInterface
  interface ScannerBuilder {
    void scan(CachedScannerResults results) throws Exception;
  }

  private Scanner buildScanner(final String tenant, final String entity, final long startTime, final ScannerBuilder builder)
      throws Exception {
    final CachedScannerResults results = new CachedScannerResults();
    builder.scan(results);
    results.sealResults(false);

    final long elapsed = System.nanoTime() - startTime;
    Logger.debug("{} scan {} read {} rows, result has {} rows {} matching the filter. took {}",
      tenant, entity != null ? entity : "ALL",
      results.getRowRead(), results.getRowCount(),
      HumansUtil.humanSize(results.getRowsSize()),
      HumansUtil.humanTimeNanos(elapsed));

    // no data...
    if (results.isEmpty()) {
      return Scanner.EMPTY;
    }

    // the scan has a single page
    final ScanResult firstResult = results.getFirstResult();
    if (results.isEmpty()) return new Scanner(firstResult);

    // the scan has multiple page
    final String scannerId = UUID.randomUUID().toString();
    scanResults.put(scannerId, results.newCachedScanResults());
    return new Scanner(scannerId, firstResult);
  }

  public ScanResult scanNext(final ScanNextRequest request) throws IOException {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    VerifyArg.verifyNotEmpty("scannerId", request.getScannerId());
    opsCount.inc(request.getTenantId());

    final CachedScanResults results = scanResults.get(request.getScannerId());
    if (results == null) return ScanResult.EMPTY_RESULT;

    final ScanResult result = results.poll();
    if (!results.hasMore()) {
      scanResults.remove(request.getScannerId());
    }

    return result != null ? result : ScanResult.EMPTY_RESULT;
  }

  public CountResult countEntity(final CountRequest request) throws Exception {
    Tracer.getCurrentTask().setTenantId(request.getTenantId());
    VerifyArg.verifyNotEmpty("groups", request.getGroups());
    opsCount.inc(request.getTenantId());

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = storage.getEntitySchema(request.getEntity());
    if (schema == null) return CountResult.EMPTY;

    final Transaction txn = storage.getTransaction(request.getTxnId());
    final CountResult result = new CountResult();

    // scan
    final QueryCache queryCache = new QueryCache();
    for (final String groupId: request.getGroups()) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), false, (row) -> {
        if (request.hasNoFilter() || Query.process(request.getFilter(), row, queryCache)) {
          result.incTotalRows();
        }
        return true;
      });
    }

    return result;
  }

  public static ByteArraySlice rowKeyEntityGroup(final EntitySchema schema, final String groupId) {
    return new RowKeyBuilder().add(groupId).add(schema.getEntityName()).addKeySeparator().slice();
  }

  private static void verifyGroups(final String[] groups) {
    VerifyArg.verifyNotEmpty("groups", groups);
    for (int i = 0; i < groups.length; ++i) {
      if (StringUtil.isEmpty(groups[i])) {
        throw new IllegalArgumentException("groups[" + i + "] = " + groups[i]);
      }
    }
  }
}
