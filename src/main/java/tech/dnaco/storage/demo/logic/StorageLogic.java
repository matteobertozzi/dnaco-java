/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.storage.demo.logic;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.gullivernet.commons.util.DateUtil;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.RowKeyUtil;
import tech.dnaco.collections.iterators.AbstractFilteredIterator;
import tech.dnaco.collections.iterators.FilteredIterator;
import tech.dnaco.collections.iterators.MergeIterator;
import tech.dnaco.collections.iterators.PeekIterator;
import tech.dnaco.collections.iterators.SimplePeekIterator;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.driver.AbstractKvStore;
import tech.dnaco.storage.demo.driver.AbstractKvStore.KeyValConsumer;
import tech.dnaco.storage.demo.driver.AbstractKvStore.RowPredicate;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;

public final class StorageLogic {
  private final ConcurrentHashMap<String, Transaction> transactions = new ConcurrentHashMap<>();

  private final AbstractKvStore storage;

  public StorageLogic(final AbstractKvStore storage) {
    this.storage = storage;
  }

  public AbstractKvStore getKvStore() {
    return storage;
  }

  public void shutdown() {
    storage.closeStorage();
  }

  public long nextCommitId() throws Exception {
    return storage.intCounter("__SYS_LAST_COMMIT_ID__");
  }

  public long getMaxCommitId() throws Exception {
    return storage.getCounterValue("__SYS_LAST_COMMIT_ID__");
  }

  public Transaction getTransaction(final String txnId) {
    return StringUtil.isEmpty(txnId) ? null : transactions.get(txnId);
  }

  public Transaction getOrCreateTransaction(final String txnId) throws Exception {
    if (StringUtil.isEmpty(txnId)) return Transaction.newLocalTxn(nextCommitId());

    final Transaction txn = transactions.get(txnId);
    if (txn != null) return txn;

    final Transaction newTxn = new Transaction(txnId, nextCommitId());
    return transactions.computeIfAbsent(txnId, (k) -> newTxn);
  }

  public EntitySchema getOrCreateEntitySchema(final String entityName) {
    final EntitySchema schema = storage.getSchema(entityName);
    if (schema == null) return new EntitySchema(entityName);
    return schema;
  }

  public EntitySchema getEntitySchema(final String entityName) {
    return storage.getSchema(entityName);
  }

  public Collection<EntitySchema> getEntitySchemas() {
    return storage.getSchemas();
  }

  public void registerSchema(final EntitySchema schema) throws Exception {
    storage.registerSchema(schema);
  }

  public void dropSchema(final String entityName) throws Exception {
    storage.dropSchema(entityName);
  }

  // ================================================================================
  //  Modification related
  // ================================================================================
  public boolean addRow(final Transaction txn, final EntityDataRow row) throws Exception {
    row.setSeqId(Long.MAX_VALUE);
    //Logger.debug("{} row {hasAllFields}: {}", row.getOperation(), row.hasAllFields(), row);
    switch (row.getOperation()) {
      case INSERT: return insertRow(txn, row);
      case UPSERT: return upsertRow(txn, row);
      case UPDATE: return updateRow(txn, row);
      case DELETE: return deleteRow(txn, row);
    }
    throw new UnsupportedOperationException();
  }

  private boolean insertRow(final Transaction txn, final EntityDataRow row) throws Exception {
    // if the row is in the transaction...
    final EntityDataRow oldTxnRow = storage.getRow(row.buildRowKey(txn.getTxnId()));
    if (oldTxnRow != null) {
      // ...if it is marked as deleted, we can insert it
      if (oldTxnRow.getOperation() == Operation.DELETE) {
        storage.put(row, txn.getTxnId());
        return true;
      }

      // ...if it is not deleted, well... already have it
      Logger.warn("{}: key already in transaction: {}", txn, row);
      addErrorRow(txn, ErrorStatus.DUPLICATE_KEY, row);
      return false;
    }

    // since the row is not in the transaction... check the master table
    final EntityDataRow oldRow = storage.getRow(row.buildRowKey());
    if (isRowActive(oldRow)) {
      Logger.warn("{}: key already present: {}", txn, row);
      addErrorRow(txn, ErrorStatus.DUPLICATE_KEY, row);
      return false;
    }

    storage.put(row, txn.getTxnId());
    return true;
  }

  private boolean upsertRow(final Transaction txn, final EntityDataRow row) throws Exception {
    // if the row has all fields, skip all the checks and write it
    if (row.hasAllFields()) {
      storage.put(row, txn.getTxnId());
      return true;
    }

    // if the row is in the transaction...
    final EntityDataRow oldTxnRow = storage.getRow(row.buildRowKey(txn.getTxnId()));
    if (oldTxnRow != null) {
      // ...if it is marked as deleted, we can replace it
      if (oldTxnRow.getOperation() == Operation.DELETE) {
        storage.put(row, txn.getTxnId());
        return true;
      }

      // ...if it is not deleted, let's replace the content
      row.mergeValues(oldTxnRow);
      storage.put(row, txn.getTxnId());
      return true;
    }

    // since the row is not in the transaction... check the master table and merge the row
    final EntityDataRow masterRow = storage.getRow(row.buildRowKey());
    if (isRowActive(masterRow)) row.mergeValues(masterRow);
    storage.put(row, txn.getTxnId());
    return true;
  }

  private boolean updateRow(final Transaction txn, final EntityDataRow row) throws Exception {
    // if the row is in the transaction...
    final EntityDataRow oldTxnRow = storage.getRow(row.buildRowKey(txn.getTxnId()));
    if (oldTxnRow != null) {
      // ...if it is marked as deleted, well... the row is not present
      if (oldTxnRow.getOperation() == Operation.DELETE) {
        Logger.warn("{}: key was deleted in this transaction: {}", txn, row);
        addErrorRow(txn, ErrorStatus.KEY_NOT_FOUND, row);
        return false;
      }

      row.mergeValues(oldTxnRow);
      storage.put(row, txn.getTxnId());
      return true;
    }

    // since the row is not in the transaction... check the master table
    final EntityDataRow oldRow = storage.getRow(row.buildRowKey());
    if (isRowDeleted(oldRow)) {
      Logger.warn("{}: key does not exists in the master: {}", txn, row);
      addErrorRow(txn, ErrorStatus.KEY_NOT_FOUND, row);
      return false;
    }

    row.mergeValues(oldRow);
    storage.put(row, txn.getTxnId());
    return true;
  }

  private boolean deleteRow(final Transaction txn, final EntityDataRow row) throws Exception {
    // if the row is in the transaction...
    final EntityDataRow oldTxnRow = storage.getRow(row.buildRowKey(txn.getTxnId()));
    if (oldTxnRow != null) {
      if (oldTxnRow.getOperation() != Operation.DELETE) {
        storage.delete(row, txn.getTxnId());
      }
      return true;
    }

    // since the row is not in the transaction...
    final EntityDataRow oldRow = storage.getRow(row.buildRowKey());
    if (isRowDeleted(oldRow)) {
      Logger.trace("{}: key does not exists in the master: {}", txn, row);
      //addErrorRow(txn, ErrorStatus.KEY_NOT_FOUND, row);
      return true;
    }

    storage.delete(row, txn.getTxnId());
    return true;
  }

  private static boolean isRowActive(final EntityDataRow row) {
    //Logger.debug("ROW: {}", row);
    return row != null && row.getOperation() != Operation.DELETE;
  }

  private static boolean isRowDeleted(final EntityDataRow row) {
    return row == null || row.getOperation() == Operation.DELETE;
  }

  // ================================================================================
  //  TXN Errors related
  // ================================================================================
  public enum ErrorStatus { KEY_NOT_FOUND, DUPLICATE_KEY, CONCURRENT_MODIFICATION }
  private void addErrorRow(final Transaction txn, final ErrorStatus status, final EntityDataRow row) {
    Logger.error("TODO: add error {} for txn {} row: {}", status, txn.getTxnId(), row);
  }

  // ================================================================================
  //  Commit/Rollback related
  // ================================================================================
  private final ReentrantLock commitLock = new ReentrantLock();
  public boolean commit(final Transaction txn) throws Exception {
    final ByteArraySlice txnKeyPrefix = EntityDataRows.buildTxnRowPrefix(txn.getTxnId());

    if (txn.getState() != Transaction.State.PENDING) {
      Logger.debug("{} was not considered for commit since it is is not pending but {}", txn.getTxnId(), txn.getState());
      return false;
    }

    final long startTime = System.nanoTime();
    commitLock.lock();
    try {
      // prepare
      final long maxCommitId = getMaxCommitId();
      Logger.debug("PREPARE {}: {txnMaxSeqId} {maxCommitId} {lockWait}",
        txn.getTxnId(), txn.getMaxSeqId(), maxCommitId, HumansUtil.humanTimeSince(startTime));
      if (maxCommitId != txn.getMaxSeqId()) {
        storage.scanRow(txnKeyPrefix, (txnRow) -> {
          final byte[] key = txnRow.buildRowKey();
          final EntityDataRow oldRow = storage.getRow(key);
          if (oldRow != null && oldRow.getSeqId() > txn.getMaxSeqId()) {
            Logger.error("concurrent modification oldRow {} txn {}", oldRow.getSeqId(), txn.getMaxSeqId());
            addErrorRow(txn, ErrorStatus.CONCURRENT_MODIFICATION, txnRow);
            txn.setState(Transaction.State.FAILED);
            return false;
          }
          return true;
        });
      }

      // TODO: update txn state
      final long commitSeqId = nextCommitId();
      final long timestamp = DateUtil.toHumanTs(ZonedDateTime.now());
      txn.setState(Transaction.State.PREPARED);

      // commit
      Logger.debug("COMMIT {}", txn.getTxnId());
      storage.scanRow(txnKeyPrefix, (row) -> {
        //System.out.println(" ----> COMMIT: " + row);
        row.setTimestamp(timestamp);
        row.setSeqId(commitSeqId);
        storage.put(row, null);
        return true;
      });
      txn.setState(Transaction.State.COMMITTED);
      Logger.debug("COMMITED {} in {}", txn.getTxnId(), HumansUtil.humanTimeSince(startTime));
      // TODO: update txn state
      storage.deletePrefix(txnKeyPrefix);
      Logger.debug("CLEANED {} in {}", txn.getTxnId(), HumansUtil.humanTimeSince(startTime));
      storage.flush();
      Logger.debug("FLUSHED {} in {}", txn.getTxnId(), HumansUtil.humanTimeSince(startTime));
      return true;
    } finally {
      commitLock.unlock();
    }
  }

  public boolean rollback(final Transaction txn) throws Exception {
    if (txn.getState() != Transaction.State.PENDING && txn.getState() != Transaction.State.FAILED) {
      Logger.debug("{} was not considered for rollback since it is {}", txn.getTxnId(), txn.getState());
      return false;
    }

    // rollback
    final ByteArraySlice txnKey = EntityDataRows.buildTxnRowPrefix(txn.getTxnId());
    Logger.debug("ROLLBACK {}", txn.getTxnId());
    storage.deletePrefix(txnKey);

    txn.setState(Transaction.State.ROLLEDBACK);
    transactions.remove(txn.getTxnId());
    return true;
  }


  // ================================================================================
  //  Drop Entity related
  // ================================================================================
  private Set<String> findEntityGroups(final String entityName) throws Exception {
    final HashSet<String> groups = new HashSet<>(64);
    scanAll((key, val) -> {
      final List<byte[]> keyParts = RowKeyUtil.decodeKey(key.buffer());
      final String group = new String(keyParts.get(0));
      if (group.startsWith(EntitySchema.SYS_TXN_PREFIX)) {
        // no-op
      } else {
        final String table = new String(keyParts.get(1));
        if (StringUtil.equals(table, entityName)) {
          groups.add(group);
        }
      }
    });
    return groups;
  }

  public void truncateEntity(final String entityName) throws Exception {
    final long startTime = System.nanoTime();

    commitLock.lock();
    try {
      Logger.debug("looking up {entityName} groups", entityName);
      final Set<String> groups = findEntityGroups(entityName);
      Logger.debug("found {} groups in entity {entityName} took {}",
        groups.size(), entityName, HumansUtil.humanTimeSince(startTime));

      truncate(entityName, groups);
      Logger.info("dropping {} groups in entity {entityName} took {}",
        groups.size(), entityName, HumansUtil.humanTimeSince(startTime));
    } finally {
      commitLock.unlock();
    }
  }

  public void dropEntity(final String entityName) throws Exception {
    final long startTime = System.nanoTime();
    truncateEntity(entityName);
    dropSchema(entityName);
    Logger.info("{entityName} dropped in {}", entityName, HumansUtil.humanTimeSince(startTime));
  }

  // ================================================================================
  //  Scan related
  // ================================================================================
  public void scanAll(final KeyValConsumer consumer) throws Exception {
    storage.scanPrefix(new ByteArraySlice(), consumer);
  }

  public PeekIterator<EntityDataRow> scanRow(final Transaction txn,
      final ByteArraySlice prefix, final boolean includeDeleted) throws Exception {
    if (txn == null) {
      if (includeDeleted) {
        return new SimplePeekIterator<>(storage.scanRow(prefix));
      }
      return new FilteredIterator<>(storage.scanRow(prefix), StorageLogic::isRowActive);
    }

    return new MergeRowIterator(List.of(
      storage.scanRow(EntityDataRows.addTxnToRowPrefix(txn.getTxnId(), prefix)),
      storage.scanRow(prefix)
    ));
  }

  public void scanRow(final Transaction txn, final ByteArraySlice prefix,
      final boolean includeDeleted, final RowPredicate consumer) throws Exception {
    final ArrayList<byte[]> toDelete = new ArrayList<>();
    final PeekIterator<EntityDataRow> it = scanRow(txn, prefix, includeDeleted);
    while (it.hasNext()) {
      final EntityDataRow row = it.next();
      if (row.getOperation() == Operation.DELETE) {
        toDelete.add(row.buildRowKey());
      }
      //System.out.println(" ---> " + row);
      if (!consumer.test(row)) {
        break;
      }
    }
    while (it.hasNext()) it.next();
  }

  public void truncate(final EntitySchema schema, final Set<String> groups) throws Exception {
    truncate(schema.getEntityName(), groups);
  }

  private void truncate(final String entityName, final Set<String> groups) throws Exception {
    for (final String groupId: groups) {
      final ByteArraySlice keyPrefix = RowKeyUtil.newKeyBuilder()
        .add(groupId)
        .add(entityName)
        .addKeySeparator()
        .slice();
      Logger.debug("deleting {entityName} records for {group}", entityName, groupId);
      storage.deletePrefix(keyPrefix);
    }
    storage.flush();
  }

  public void cleanup(final EntitySchema schema, final Set<String> groups) throws Exception {
    for (final String groupId: groups) {
      final ArrayList<byte[]> toDelete = new ArrayList<>();
      final ByteArraySlice key = RowKeyUtil.newKeyBuilder().add(groupId).add(schema.getEntityName()).addKeySeparator().slice();
      Logger.debug("loookup rows for {scheam} {group}", schema.getEntityName(), groupId);
      storage.scanRow(key, row -> {
        if (row.getOperation() == Operation.DELETE) {
          toDelete.add(row.buildRowKey());
        }
        return true;
      });

      Logger.debug("deleting {} rows for {scheam} {group}", toDelete.size(), schema.getEntityName(), groupId);
      for (final byte[] dk: toDelete) {
        storage.deletePrefix(new ByteArraySlice(dk));
      }
      Logger.debug("deleted {} rows for {scheam} {group}", toDelete.size(), schema.getEntityName(), groupId);
    }
  }

  public EntityDataRow getRow(final Transaction txn, final EntityDataRow row) throws Exception {
    if (txn != null) {
      final EntityDataRow txnRow = storage.getRow(row.buildRowKey(txn.getTxnId()));
      if (txnRow != null) return (txnRow.getOperation() == Operation.DELETE) ? null : txnRow;
    }

    final EntityDataRow masterRow = storage.getRow(row.buildRowKey());
	  return isRowActive(masterRow) ? masterRow : null;
  }

  private static final class MergeRowIterator extends AbstractFilteredIterator<EntityDataRow, EntityDataRow> {
    public MergeRowIterator(final List<Iterator<EntityDataRow>> iterators) {
      super(new MergeIterator<>(iterators, EntityDataRow::compareKeyAndSeq));
    }

    @Override
    protected void computeNext() {
      setNoMoreItems();
      while (iterator.hasNext()) {
        final EntityDataRow lastRow = iterator.next();
        //System.out.println(" -> FETCH ROW: " + lastRow);
        while (iterator.hasNext()) {
          //System.out.println(" ----> PEEK ROW: " + peekNext() + " --> " + lastRow);
          if (EntityDataRow.compareKey(lastRow, peekNext()) != 0) {
            break;
          }
          // ignore other keys (the first one is the most updated one)
          iterator.next();
        }

        if (isRowActive(lastRow)) {
          setNextItem(lastRow);
          break;
        }
      }
    }
  }
}
