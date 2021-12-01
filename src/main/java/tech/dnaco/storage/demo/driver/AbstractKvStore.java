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

package tech.dnaco.storage.demo.driver;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.iterators.AbstractFilteredIterator;
import tech.dnaco.collections.iterators.SimplePeekIterator;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.RowKeyUtil;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.strings.HumansUtil;

public abstract class AbstractKvStore {
  protected static final byte[] SYS_COUNTERS = RowKeyUtil.newKeyBuilder().add("__SYS__").add("counter").drain();
  protected static final byte[] SYS_SCHEMAS = RowKeyUtil.newKeyBuilder().add("__SYS__").add("schema").drain();

  private final ConcurrentHashMap<String, EntitySchema> schemas = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();
  private final String projectId;

  protected AbstractKvStore(final String projectId) {
    this.projectId = projectId;
  }

  public String getProjectId() {
    return projectId;
  }

  // ================================================================================
  //  Init Related
  // ================================================================================
  public void loadStorage() throws Exception {
    final long startTime = System.nanoTime();

    openKvStore();

    loadCounters();
    loadSchemas();
    resumePendingCommits();

    Logger.debug("{} storage loaded in {}", projectId, HumansUtil.humanTimeSince(startTime));
  }

  public void closeStorage() {
    shutdownKvStore();
  }

  protected abstract void openKvStore() throws Exception;
  protected abstract void shutdownKvStore();

  private void loadCounters() throws Exception {
    final long startTime = System.nanoTime();
    final Iterator<Entry<ByteArraySlice, byte[]>> it = this.scanPrefix(SYS_COUNTERS);
    while (it.hasNext()) {
      final Entry<ByteArraySlice, byte[]> entry = it.next();
      final byte[] bCounterName = RowKeyUtil.lastKeyComponent(entry.getKey().buffer());
      final String counterName = new String(bCounterName);

      final long intValue = IntDecoder.BIG_ENDIAN.readFixed64(entry.getValue(), 0);
      this.counters.put(counterName, intValue);
      Logger.debug("load counter {}: {}", counterName, intValue);
    }
    Logger.debug("load counters took {}", HumansUtil.humanTimeSince(startTime));
  }

  private void loadSchemas() throws Exception {
    final long startTime = System.nanoTime();
    final Iterator<Entry<ByteArraySlice, byte[]>> it = this.scanPrefix(SYS_SCHEMAS);
    while (it.hasNext()) {
      final Entry<ByteArraySlice, byte[]> entry = it.next();
      final byte[] bEntityName = RowKeyUtil.lastKeyComponent(entry.getKey().buffer());
      final String entityName = new String(bEntityName);

      final EntitySchema schema = EntitySchema.decode(entry.getValue());
      this.schemas.put(entityName, schema);
      Logger.debug("load schema {}: {}", entityName, schema);
    }
    Logger.debug("load schemas took {}", HumansUtil.humanTimeSince(startTime));
  }

  private void resumePendingCommits() {
    // TODO
    Logger.warn("TODO: Resume Pending Commits");
  }

  // ================================================================================
  //  Schema Related
  // ================================================================================
  public void registerSchema(final EntitySchema schema) throws Exception {
    final RowKeyBuilder key = new RowKeyBuilder();
    key.add("__SYS__");
    key.add("schema");
    key.add(schema.getEntityName());
    put(key.slice(), schema.encode());

    schemas.put(schema.getEntityName(), schema);
  }

  public void dropSchema(final String entityName) throws Exception {
    final RowKeyBuilder key = new RowKeyBuilder();
    key.add("__SYS__");
    key.add("schema");
    key.add(entityName);
    delete(key.slice());
  }

  public EntitySchema getSchema(final byte[] entityName) {
    return getSchema(new String(entityName));
  }

  public EntitySchema getSchema(final String entityName) {
    return schemas.get(entityName);
  }

  public Collection<EntitySchema> getSchemas() {
    return schemas.values();
  }

  public Set<String> getEntityNames() {
    return schemas.keySet();
  }

  // ================================================================================
  //  Counters Related
  // ================================================================================
  public long intCounter(final String counterName) throws Exception {
    return addToCounter(counterName, 1);
  }

  public long addToCounter(final String counterName, final long amount) throws Exception {
    final byte[] value = new byte[8];
    final RowKeyBuilder tableKey = RowKeyUtil.newKeyBuilder(SYS_COUNTERS);
    synchronized (this) {
      final long intValue = this.counters.getOrDefault(counterName, 0L) + 1;
      IntEncoder.BIG_ENDIAN.writeFixed64(value, 0, intValue);
      put(tableKey.add(counterName).slice(), value);
      this.counters.put(counterName, intValue);
      return intValue;
    }
  }

  public long getCounterValue(final String counterName) throws Exception {
    return this.counters.getOrDefault(counterName, 0L);
  }

  // ================================================================================
  //  Put/Delete Related
  // ================================================================================
  public abstract void put(final ByteArraySlice key, final byte[] value) throws Exception;
  public abstract void delete(final ByteArraySlice key) throws Exception;
  public abstract void deletePrefix(final ByteArraySlice keyPrefix) throws Exception;
  public abstract void flush() throws Exception;

  public void put(final EntityDataRow row, final String txnId) throws Exception {
    preparePutEntries(row, txnId, this::put);
  }

  public void put(final EntityDataRows rows, final String txnId) throws Exception {
    preparePutEntries(rows, txnId, this::put);
  }

  protected void preparePutEntries(final EntityDataRows rows, final String txnId, final KeyValConsumer consumer) throws Exception {
    for (int row = 0, n = rows.rowCount(); row < n; ++row) {
      preparePutEntries(new EntityDataRow(rows, row), txnId, consumer);
    }
  }

  protected void preparePutEntries(final EntityDataRow row, final String txnId, final KeyValConsumer consumer) throws Exception {
    final EntitySchema schema = row.getSchema();
    final byte[] rowKey = row.buildRowKey(txnId);

    final List<String> fields = schema.getNonKeyFields();
    for (int i = fields.size() - 1; i >= 0; --i) {
      final String field = fields.get(i);
      final ByteArraySlice key = new RowKeyBuilder(rowKey).add(field).slice();
      final byte[] val = row.get(field);
      //Logger.debug("prepare {} {}", field, row);
      if (val != null) consumer.accept(key, val);
    }
  }

  public void delete(final EntityDataRow row, final String txnId) throws Exception {
    final EntitySchema schema = row.getSchema();
    final byte[] rowKey = row.buildRowKey(txnId);

    final List<String> fields = schema.getNonKeyFields();
    for (int i = fields.size() - 1; i >= 0; --i) {
      final String field = fields.get(i);
      final ByteArraySlice key = new RowKeyBuilder(rowKey).add(field).slice();
      if (schema.isSysField(field)) {
        final byte[] val = row.get(field);
        //System.out.println("DEL-PUT " + key);
        this.put(key, val);
      } else {
        //System.out.println("DELETE " + key);
        this.delete(key);
      }
    }
  }

  public void delete(final EntityDataRows rows, final String txnId) throws Exception {
    for (int row = 0, n = rows.rowCount(); row < n; ++row) {
      delete(new EntityDataRow(rows, row), txnId);
    }
  }

  public interface RawKeyValConsumer {
    void accept(byte[] key, byte[] value) throws Exception;
  }

  public interface KeyValConsumer {
    void accept(ByteArraySlice key, byte[] value) throws Exception;
  }

  public interface RowPredicate {
    boolean test(EntityDataRow row) throws Exception;
  }

  // ================================================================================
  //  Scan Related
  // ================================================================================
  public EntityDataRow getRow(final byte[] key) throws Exception {
    return getRow(new ByteArraySlice(key));
  }

  public EntityDataRow getRow(final ByteArraySlice key) throws Exception {
    final AtomicReference<EntityDataRow> rowRef = new AtomicReference<>();
    scanRow(key, (row) -> {
      rowRef.set(row);
      return false;
    });
    return rowRef.get();
  }

  public Iterator<EntityDataRow> scanRow(final ByteArraySlice prefix) throws Exception {
    return new RowIterator(scanPrefix(prefix));
  }

  public void scanRow(final ByteArraySlice prefix, final RowPredicate predicate) throws Exception {
    final Iterator<EntityDataRow> it = scanRow(prefix);
    while (it.hasNext()) {
      final EntityDataRow row = it.next();
      if (!predicate.test(row)) break;
    }
    // TODO: we must close the iterator
    //while (it.hasNext()) it.next();
  }

  public void scanPrefix(final ByteArraySlice prefix, final KeyValConsumer consumer) throws Exception {
    final Iterator<Entry<ByteArraySlice, byte[]>> it = scanPrefix(prefix);
    while (it.hasNext()) {
      final Entry<ByteArraySlice, byte[]> entry = it.next();
      consumer.accept(entry.getKey(), entry.getValue());
    }
    // TODO: we must close the iterator
    //while (it.hasNext()) it.next();
  }

  protected Iterator<Entry<ByteArraySlice, byte[]>> scanPrefix(final byte[] prefix) throws Exception {
    return scanPrefix(new ByteArraySlice(prefix));
  }

  protected abstract Iterator<Entry<ByteArraySlice, byte[]>> scanPrefix(final ByteArraySlice prefix) throws Exception;

  protected final class RowIterator extends AbstractFilteredIterator<Entry<ByteArraySlice, byte[]>, EntityDataRow> {
    public RowIterator(final Iterator<Entry<ByteArraySlice, byte[]>> iterator) {
      super(SimplePeekIterator.newIterator(iterator));
    }

    @Override
    protected void computeNext() {
      if (!iterator.hasNext()) {
        setNoMoreItems();
        return;
      }

      byte[] nextKey = this.peekNext().getKey().buffer();

      final ByteArraySlice rowKey = RowKeyUtil.keyWithoutLastComponent(nextKey);
      //System.out.println(" ----> ROW KEY: " + rowKey);
      final List<byte[]> keyParts = RowKeyUtil.decodeKey(rowKey.buffer());

      final EntityDataRows rows;
      if (BytesUtil.hasPrefix(keyParts.get(0), 0, keyParts.get(0).length, EntityDataRows.SYS_TXN_PREFIX, 0, EntityDataRows.SYS_TXN_PREFIX.length)) {
        // [txn].[group].[entity].[key].[field]
        final EntitySchema schema = getSchema(keyParts.get(2));
        rows = new EntityDataRows(schema, false).newRow();
        rows.addTxnKey(keyParts);
      } else {
        // [group].[entity].[key].[field]
        final EntitySchema schema = getSchema(keyParts.get(1));
        if (schema == null) throw new UnsupportedOperationException("invalid schema " + new String(keyParts.get(0)) + "." + new String(keyParts.get(1)));
        rows = new EntityDataRows(schema, false).newRow();
        rows.addKey(keyParts);
      }

      do {
        nextKey = this.peekNext().getKey().buffer();
        final ByteArraySlice nextRowKey = RowKeyUtil.keyWithoutLastComponent(nextKey);
        if (!rowKey.equals(nextRowKey)) break;

        final Entry<ByteArraySlice, byte[]> entry = iterator.next();
        final byte[] fieldName = RowKeyUtil.lastKeyComponent(entry.getKey().buffer());
        rows.add(new String(fieldName), entry.getValue());
        //System.out.println(" --------> FIELD: " + new String(fieldName));
      } while (iterator.hasNext());
      setNextItem(new EntityDataRow(rows, 0));
    }
  }

  // ================================================================================
  //  Key Util
  // ================================================================================
  public static ByteArraySlice prefixEndKey(final ByteArraySlice prefix) throws Exception {
    return new ByteArraySlice(prefixEndKey(prefix.buffer()));
  }

  public static byte[] prefixEndKey(final byte[] prefix) throws Exception {
    final byte[] endKey = Arrays.copyOf(prefix, prefix.length);
    return increaseOne(endKey);
  }

  private static byte[] increaseOne(final byte[] bytes) throws Exception {
    final byte BYTE_MAX_VALUE = (byte) 0xff;
    assert bytes.length > 0;
    final byte last = bytes[bytes.length - 1];
    if (last != BYTE_MAX_VALUE) {
      bytes[bytes.length - 1] += 0x01;
    } else {
      // Process overflow (like [1, 255] => [2, 0])
      int i = bytes.length - 1;
      for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
          bytes[i] += 0x01;
      }
      if (bytes[i] == BYTE_MAX_VALUE) {
        assert i == 0;
        throw new Exception("Unable to increase bytes: " + BytesUtil.toHexBytes(bytes));
      }
      bytes[i] += 0x01;
    }
    return bytes;
  }
}
