package tech.dnaco.storage.net;

import java.time.ZonedDateTime;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.gullivernet.commons.util.DateUtil;
import com.gullivernet.commons.util.VerifyArg;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.storage.demo.logic.Query;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;
import tech.dnaco.storage.net.models.CountRequest;
import tech.dnaco.storage.net.models.CountResult;
import tech.dnaco.storage.net.models.JsonEntityDataRows;
import tech.dnaco.storage.net.models.ModificationRequest;
import tech.dnaco.storage.net.models.ModificationWithFilterRequest;
import tech.dnaco.storage.net.models.ScanNextRequest;
import tech.dnaco.storage.net.models.ScanRequest;
import tech.dnaco.storage.net.models.ScanResult;
import tech.dnaco.storage.net.models.Scanner;
import tech.dnaco.storage.net.models.TransactionCommitRequest;
import tech.dnaco.storage.net.models.TransactionStatusResponse;
import tech.dnaco.strings.HumansUtil;

public final class EntityStorage {
  public static final EntityStorage INSTANCE = new EntityStorage();

  // ================================================================================
  //  Modification Handlers
  // ================================================================================
  public TransactionStatusResponse insertEntity(final ModificationRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    return modify(request, Operation.INSERT);
  }

  public TransactionStatusResponse upsertEntity(final ModificationRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    return modify(request, Operation.UPSERT);
  }

  public TransactionStatusResponse updateEntity(final ModificationRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    return modify(request, Operation.UPDATE);
  }

  public TransactionStatusResponse updateEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    final long startTime = System.nanoTime();

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = updateSchema(storage, request.getEntity(), null, new JsonEntityDataRows[] { request.getFieldsToUpdate() });
    if (schema == null) {
      return new TransactionStatusResponse(Transaction.State.FAILED);
    }

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.getTxnId());

    for (final String groupId: request.getGroups()) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.hasNoFilter() || Query.process(request.getFilter(), row)) {
          final EntityDataRows updatedRow = new EntityDataRows(schema).newRow();
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
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    return modify(request, Operation.DELETE);
  }

  public TransactionStatusResponse deleteEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    final long startTime = System.nanoTime();

    final StorageLogic storage = Storage.getInstance(request.getTenantId());
    final EntitySchema schema = storage.getOrCreateEntitySchema(request.getEntity());

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.getTxnId());

    for (final String groupId: request.getGroups()) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.hasNoFilter() || Query.process(request.getFilter(), row)) {
          final EntityDataRows updatedRow = new EntityDataRows(schema).newRow();
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
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
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

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = updateSchema(storage, request.getEntity(), request.getKeys(), request.getRows());
    if (schema == null) {
      return new TransactionStatusResponse(Transaction.State.FAILED);
    }

    // add to TXN log
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
        return true;
      });
    }

    commitIfLocalTxn(storage, txn);

    final long elapsed = System.nanoTime() - startTime;
    Logger.debug("{} {} {} took {}",
      operation, request.getTenantId(), schema.getEntityName(), HumansUtil.humanTimeNanos(elapsed));

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
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    return scanEntity(request);
  }

  public Scanner scanEntity(final ScanRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    VerifyArg.verifyNotEmpty("groups", request.getGroups());

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
      for (final String groupId: request.getGroups()) {
        storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
          if (request.hasNoFilter() || Query.process(request.getFilter(), row)) {
            results.add(row);
            results.rowCount++;
          }
          results.rowRead++;
          return true;
        });
      }
    });
  }

  private final ConcurrentHashMap<String, Queue<ScanResult>> scanResults = new ConcurrentHashMap<>();
  public Scanner scanAll(final ScanRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    VerifyArg.verifyNotEmpty("groups", request.getGroups());

    final long startTime = System.nanoTime();
    final StorageLogic storage = Storage.getInstance(request.getTenantId());
    final Transaction txn = storage.getTransaction(request.getTxnId());

    return buildScanner(request.getTenantId(), null, startTime, (results) -> {
      for (final String groupId: request.getGroups()) {
        storage.scanRow(txn, new RowKeyBuilder().add(groupId).addKeySeparator().slice(), (row) -> {
          if (request.hasNoFilter() || Query.process(request.getFilter(), row)) {
            results.add(row);
            results.rowCount++;
          }
          results.rowRead++;
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
    Logger.debug("{} scan {} read {} rows, result has {} rows matching the filter. took {}",
      tenant, entity != null ? entity : "ALL", results.rowRead, results.rowCount, HumansUtil.humanTimeNanos(elapsed));

    // no data...
    if (results.isEmpty()) {
      return Scanner.EMPTY;
    }

    // the scan has a single page
    final ScanResult firstResult = results.scanner.poll();
    if (results.isEmpty()) return new Scanner(firstResult);

    // the scan has multiple page
    final String scannerId = UUID.randomUUID().toString();
    scanResults.put(scannerId, results.scanner);
    return new Scanner(scannerId, firstResult);
  }

  public ScanResult scanNext(final ScanNextRequest request) {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    final Queue<ScanResult> results = scanResults.get(request.getScannerId());
    if (results == null) return ScanResult.EMPTY_RESULT;

    final ScanResult result = results.poll();
    if (results.isEmpty()) scanResults.remove(request.getScannerId());

    return result != null ? result : ScanResult.EMPTY_RESULT;
  }

  public static final class CachedScannerResults {
    private final LinkedBlockingQueue<ScanResult> scanner = new LinkedBlockingQueue<>();
    private EntitySchema schema;
    private ScanResult results;
    private long rowCount;
    private long rowRead;

    public boolean isEmpty() {
      return scanner.isEmpty();
    }

    public void sealResults() {
      sealResults(true);
    }

    public void sealResults(final boolean hasMore) {
      if (results != null) {
        results.setMore(hasMore);
        scanner.add(results);
      }
    }

    public boolean isSameSchema(final EntitySchema other) {
      return schema != null && this.schema.getEntityName().equals(other.getEntityName());
    }

    public void setSchema(final EntitySchema schema) {
      sealResults();

      this.schema = schema;
      this.results = new ScanResult(schema);
    }

    public void add(final EntityDataRow row) {
      if (!isSameSchema(row.getSchema())) {
        setSchema(row.getSchema());
      }
      results.add(row);
    }
  }

  public CountResult countEntity(final CountRequest request) throws Exception {
    Logger.setSession(LoggerSession.newSession(request.getTenantId(), Logger.getSession()));
    VerifyArg.verifyNotEmpty("groups", request.getGroups());

    final StorageLogic storage = Storage.getInstance(request.getTenantId());

    final EntitySchema schema = storage.getEntitySchema(request.getEntity());
    if (schema == null) return CountResult.EMPTY;

    final Transaction txn = storage.getTransaction(request.getTxnId());
    final CountResult result = new CountResult();

    // scan
    for (final String groupId: request.getGroups()) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.hasNoFilter() || Query.process(request.getFilter(), row)) {
          result.incTotalRows();
        }
        return true;
      });
    }

    return result;
  }

  private static ByteArraySlice rowKeyEntityGroup(final EntitySchema schema, final String groupId) {
    return new RowKeyBuilder().add(groupId).add(schema.getEntityName()).addKeySeparator().slice();
  }
}
