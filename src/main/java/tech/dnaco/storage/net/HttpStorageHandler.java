package tech.dnaco.storage.net;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.gullivernet.commons.util.DateUtil;
import com.gullivernet.commons.util.VerifyArg;
import com.gullivernet.server.http.HttpMethod;
import com.gullivernet.server.http.rest.RestRequestHandler;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.collections.HashIndexedArray;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntityDataType;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.Filter;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.storage.demo.driver.AbstractKvStore.RowPredicate;
import tech.dnaco.storage.demo.logic.Query;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.demo.logic.StorageLogic;
import tech.dnaco.storage.demo.logic.Transaction;

public class HttpStorageHandler implements RestRequestHandler {
  // ================================================================================
  //  Modification Handlers
  // ================================================================================
  @UriMapping(uri = "/v0/entity/insert", method = HttpMethod.POST)
  public TransactionStatusResponse insertEntity(final ModificationRequest request) throws Exception {
    return modify(request, Operation.INSERT);
  }

  @UriMapping(uri = "/v0/entity/upsert", method = HttpMethod.POST)
  public TransactionStatusResponse upsertEntity(final ModificationRequest request) throws Exception {
    return modify(request, Operation.UPSERT);
  }

  @UriMapping(uri = "/v0/entity/update", method = HttpMethod.POST)
  public TransactionStatusResponse updateEntity(final ModificationRequest request) throws Exception {
    return modify(request, Operation.UPDATE);
  }

  @UriMapping(uri = "/v0/entity/update-filtered", method = HttpMethod.POST)
  public TransactionStatusResponse updateEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.tenantId);

    final EntitySchema schema = updateSchema(storage, request.entity, null, new JsonEntityDataRows[] { request.fieldsToUpdate });
    if (schema == null) {
      return new TransactionStatusResponse(Transaction.State.FAILED);
    }

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.txnId);

    for (final String groupId: request.groups) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.filter == null || Query.process(request.filter, row)) {
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

    return new TransactionStatusResponse(txn.getState());
  }

  @UriMapping(uri = "/v0/entity/delete", method = HttpMethod.POST)
  public TransactionStatusResponse deleteEntity(final ModificationRequest request) throws Exception {
    return modify(request, Operation.DELETE);
  }

  @UriMapping(uri = "/v0/entity/delete-filtered", method = HttpMethod.POST)
  public TransactionStatusResponse deleteEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.tenantId);

    final EntitySchema schema = storage.getEntitySchema(request.entity);

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.txnId);

    for (final String groupId: request.groups) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.filter == null || Query.process(request.filter, row)) {
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

    return new TransactionStatusResponse(txn.getState());
  }

  @UriMapping(uri = "/v0/commit", method = HttpMethod.POST)
  public TransactionStatusResponse commit(final TransactionCommitRequest request) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.tenantId);

    final Transaction txn = storage.getTransaction(request.txnId);
    if (txn == null) throw new IllegalArgumentException("invalid txnId");

    if (request.rollback) {
      storage.rollback(txn);
    } else {
      storage.commit(txn);
    }
    return new TransactionStatusResponse(txn.getState());
  }

  private EntitySchema updateSchema(final StorageLogic storage, final String entity,
      final String[] keys, final JsonEntityDataRows[] rows) throws Exception {
    // apply schema checks and modifications
    final EntitySchema schema = storage.getEntitySchema(entity);
    if (keys != null && !schema.setKey(keys)) {
      // TODO: schema mismatch
      return null;
    }

    for (final JsonEntityDataRows jsonRows: rows) {
      if (!schema.update(jsonRows.fieldNames.keySet(), jsonRows.types)) {
        // TODO: schema mismatch
        return null;
      }
    }

    // update the schema
    storage.registerSchema(schema);
    return schema;
  }

  private TransactionStatusResponse modify(final ModificationRequest request, final Operation operation) throws Exception {
    final StorageLogic storage = Storage.getInstance(request.tenantId);

    final EntitySchema schema = updateSchema(storage, request.entity, request.keys, request.rows);
    if (schema == null) {
      return new TransactionStatusResponse(Transaction.State.FAILED);
    }

    // add to TXN log
    final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
    final Transaction txn = storage.getOrCreateTransaction(request.txnId);
    for (final JsonEntityDataRows jsonRows: request.rows) {
      if (txn.getState() != Transaction.State.PENDING) break;

      jsonRows.forEachEntityRow(schema, request.groups, (row) -> {
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
  @UriMapping(uri = "/v0/entity/get", method = HttpMethod.POST)
  public Scanner getEntity(final ScanRequest request) throws Exception {
    return scanEntity(request);
  }

  @UriMapping(uri = "/v0/entity/scan", method = HttpMethod.POST)
  public Scanner scanEntity(final ScanRequest request) throws Exception {
    VerifyArg.verifyNotEmpty("groups", request.groups);

    final StorageLogic storage = Storage.getInstance(request.tenantId);

    final EntitySchema schema = storage.getEntitySchema(request.entity);
    if (schema == null) throw new UnsupportedOperationException();

    final Transaction txn = storage.getTransaction(request.txnId);
    final ScanResult result = new ScanResult(schema, request.fields);

    // direct get calls
    if (ArrayUtil.isNotEmpty(request.rows)) {
      for (final JsonEntityDataRows jsonRows: request.rows) {
        if (txn.getState() != Transaction.State.PENDING) break;

        jsonRows.forEachEntityRow(schema, request.groups, (row) -> {
          result.add(storage.getRow(txn, row));
          return true;
        });
      }
    }

    // scan
    for (final String groupId: request.groups) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.filter == null || Query.process(request.filter, row)) {
          result.add(row);
        }
        return true;
      });
    }

    return new Scanner(result);
  }

  private final ConcurrentHashMap<String, Queue<ScanResult>> scanResults = new ConcurrentHashMap<>();

  @UriMapping(uri = "/v0/entity/scan-all", method = HttpMethod.POST)
  public Scanner scanAll(final ScanRequest request) throws Exception {
    VerifyArg.verifyNotEmpty("groups", request.groups);

    final StorageLogic storage = Storage.getInstance(request.tenantId);
    final Transaction txn = storage.getTransaction(request.txnId);

    final CachedScannerResults results = new CachedScannerResults();
    for (final String groupId: request.groups) {
      storage.scanRow(txn, new RowKeyBuilder().add(groupId).addKeySeparator().slice(), (row) -> {
        if (request.filter == null || Query.process(request.filter, row)) {
          results.add(row);
        }
        return true;
      });
    }
    results.sealResults();

    if (results.isEmpty()) {
      return Scanner.EMPTY;
    }

    final String scannerId = UUID.randomUUID().toString();
    scanResults.put(scannerId, results.scanner);

    return new Scanner(scannerId, results.scanner.poll());
  }

  @UriMapping(uri = "/v0/entity/scan-next", method = HttpMethod.POST)
  public ScanResult scanEntity(final ScanNextRequest request) {
    final Queue<ScanResult> results = scanResults.get(request.scannerId);
    if (results == null) return ScanResult.EMPTY_RESULT;

    final ScanResult result = results.poll();
    if (results.isEmpty()) scanResults.remove(request.scannerId);

    return result;
  }


  private static final class CachedScannerResults {
    private final LinkedBlockingQueue<ScanResult> scanner = new LinkedBlockingQueue<>();
    private EntitySchema schema;
    private ScanResult results;

    public boolean isEmpty() {
      return scanner.isEmpty();
    }

    public void sealResults() {
      if (results != null) {
        results.more = true;
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

  @UriMapping(uri = "/v0/entity/count", method = HttpMethod.POST)
  public CountResult countEntity(final CountRequest request) throws Exception {
    VerifyArg.verifyNotEmpty("groups", request.groups);

    final StorageLogic storage = Storage.getInstance(request.tenantId);

    final EntitySchema schema = storage.getEntitySchema(request.entity);
    if (schema == null) throw new UnsupportedOperationException();

    final Transaction txn = storage.getTransaction(request.txnId);
    final CountResult result =  new CountResult();

    // scan
    for (final String groupId: request.groups) {
      storage.scanRow(txn, rowKeyEntityGroup(schema, groupId), (row) -> {
        if (request.filter == null || Query.process(request.filter, row)) {
          result.totalRows++;
        }
        return true;
      });
    }

    return result;
  }

  private static ByteArraySlice rowKeyEntityGroup(final EntitySchema schema, final String groupId) {
    return new RowKeyBuilder().add(groupId).add(schema.getEntityName()).addKeySeparator().slice();
  }

  // ================================================================================
  //  RPC Models
  // ================================================================================
  private static final class TransactionCommitRequest {
    private String tenantId;
    private String txnId;
    private boolean rollback;
  }

  private static final class TransactionStatusResponse {
    private final Transaction.State state;

    public TransactionStatusResponse(final Transaction.State state) {
      this.state = state;
    }
  }

  public static final class ModificationRequest {
    private String tenantId;
    private String txnId;
    private String entity;
    private String[] groups;
    private String[] keys;
    private JsonEntityDataRows[] rows;

    public boolean hasData() {
      return ArrayUtil.isNotEmpty(rows);
    }
  }

  public static final class ModificationWithFilterRequest {
    private String tenantId;
    private String txnId;
    private String entity;
    private String[] groups;
    private JsonEntityDataRows fieldsToUpdate;
    private Filter filter;
  }

  private static final class CountRequest {
    private String tenantId;
    private String txnId;
    private String entity;
    private String[] groups;
    private Filter filter;
  }

  private static final class CountResult {
    private long totalRows;
  }

  private static final class ScanRequest {
    private String tenantId;
    private String txnId;
    private String entity;
    private String[] groups;
    private String[] fields;
    private JsonEntityDataRows[] rows;
    private Filter filter;
  }

  public static final class Scanner {
    private static final Scanner EMPTY = new Scanner(null);

    private final ScanResult result;
    private final String scannerId;

    public Scanner(final ScanResult result) {
      this(null, result);
    }

    public Scanner(final String scannerId, final ScanResult result) {
      this.result = result;
      this.scannerId = scannerId;
    }
  }

  private static final class ScanNextRequest {
    private String tenantId;
    private String scannerId;
  }

  public static final class ScanResult {
    public static final ScanResult EMPTY_RESULT = new ScanResult();

    private final JsonEntityDataRows rows;
    private String[] key;
    private String entity;
    private boolean more = false;

    private ScanResult() {
      this.rows = null;
      this.more = false;
    }

    public ScanResult(final EntitySchema schema) {
      this(schema, null);
    }

    public ScanResult(final EntitySchema schema, final String[] fields) {
      final String[] resultFields;
      if (ArrayUtil.isEmpty(fields)) {
        // TODO: remove system fields e.g. __group__, __seqid__
        resultFields = schema.getFieldNames().toArray(new String[0]);
      } else {
        resultFields = fields;
      }
      Arrays.sort(resultFields);

      this.rows = new JsonEntityDataRows(schema, resultFields);
      this.key = schema.getKeyFields();
      this.entity = schema.getEntityName();
    }

    public void add(final EntityDataRow row) {
      rows.add(row);
    }
  }

  private static final class JsonEntityDataRows {
    private HashIndexedArray<String> fieldNames;
    private EntityDataType[] types;
    private Object[] values;

    public JsonEntityDataRows() {
      // no-op
    }

    public JsonEntityDataRows(final EntitySchema schema, final String[] fieldNames) {
      this.fieldNames = new HashIndexedArray<>(fieldNames);
      this.types = new EntityDataType[fieldNames.length];
      for (int i = 0; i < fieldNames.length; ++i) {
        this.types[i] = schema.getFieldType(fieldNames[i]);
      }
      this.values = new Object[0];
    }

    public void add(final EntityDataRow row) {
      final String[] fields = fieldNames.keySet();
      final int rowOffset = values.length;
      this.values = Arrays.copyOf(values, rowOffset + fieldNames.size());
      for (int i = 0; i < fields.length; ++i) {
        values[rowOffset + i] = row.getObject(fields[i]);
      }
    }

    public boolean forEachEntityRow(final EntitySchema schema, final String[] groups,
        final RowPredicate consumer) throws Exception {
      final int fieldCount = fieldNames.size();
      for (int i = 0, n = rowCount(); i < n; ++i) {
        final int rowOffset = i * fieldCount;
        for (final String groupId: groups) {
          final EntityDataRows rows = new EntityDataRows(schema).newRow();
          rows.add(EntitySchema.SYS_FIELD_GROUP, groupId);
          rows.add(EntitySchema.SYS_FIELD_SEQID, Long.MAX_VALUE);
          for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
            rows.add(fieldNames.get(fieldIndex), values[rowOffset + fieldIndex]);
          }

          if (!consumer.test(new EntityDataRow(rows, 0))) {
            return false;
          }
        }
      }
      return true;
    }

    public int rowCount() {
      return values.length / fieldNames.size();
    }
  }
}
