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

import java.io.File;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.iterators.PeekIterator;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.strings.HumansUtil;

public class RocksDbKvStore extends AbstractKvStore {
  private static BlockBasedTableConfig tableConfig = null;
  private static File storageDir;

  private Options dbOptions;
  private RocksDB db;

  public RocksDbKvStore(final String projectId) {
		super(projectId);
  }

  // ================================================================================
  //  RocksDB Init Related
  // ================================================================================
  public static void init(final File rootDir, final long cacheSize) {
    storageDir = rootDir;

    // Init RocksDb Library
    RocksDB.loadLibrary();

    // init shared block cache
    tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockCache(new LRUCache(cacheSize));
    tableConfig.setBlockSize(8 << 10);
    tableConfig.setWholeKeyFiltering(false);
    Logger.debug("init with LRU Cache Size: {}", HumansUtil.humanSize(cacheSize));
  }

  // ================================================================================
  //  Init Related
  // ================================================================================
  @Override
  public void openKvStore() throws Exception {
    dbOptions = new Options();
    dbOptions.setTableFormatConfig(tableConfig);
    dbOptions.setWriteBufferSize(16 << 20);
    dbOptions.setCompressionType(CompressionType.ZSTD_COMPRESSION);
    dbOptions.setCompactionStyle(CompactionStyle.LEVEL);
    dbOptions.setMaxWriteBufferNumber(2);
    dbOptions.setOptimizeFiltersForHits(true);
    dbOptions.setCreateIfMissing(true);
    dbOptions.setKeepLogFileNum(1);
    dbOptions.setLogFileTimeToRoll(Duration.ofDays(1).toSeconds());

    final File dbPath = new File(storageDir, getProjectId());
    Logger.debug("loading {} store from {}", getProjectId(), dbPath);
    dbPath.getParentFile().mkdirs();
    db = RocksDB.open(dbOptions, dbPath.getAbsolutePath());
    compact();
  }

  @Override
  protected void shutdownKvStore() {
    try {
      try (FlushOptions opts = new FlushOptions()) {
        opts.setAllowWriteStall(true);
        db.flush(opts);
      }
      Logger.debug("flushing {} store", getProjectId());
    } catch (final Exception e) {
      Logger.error(e, "failed to flush db {}", getProjectId());
    }
    IOUtil.closeQuietly(db);
    IOUtil.closeQuietly(dbOptions);
  }

  // ================================================================================
  //  Put/Delete Related
  // ================================================================================
	@Override
	public void put(final ByteArraySlice key, final byte[] value) throws Exception {
    try (WriteOptions writeOpts = new WriteOptions()) {
      writeOpts.setDisableWAL(false);
      db.put(writeOpts, key.buffer(), value);
    }
  }

  @Override
  public void put(final EntityDataRow row, final String txnId) throws Exception {
    try (WriteOptions writeOpts = new WriteOptions()) {
      writeOpts.setDisableWAL(false);
      try (WriteBatch batch = new WriteBatch()) {
        preparePutEntries(row, txnId, (k, v) -> batch.put(k.buffer(), v));
        db.write(writeOpts, batch);
      }
    }
  }

  @Override
  public void put(final EntityDataRows rows, final String txnId) throws Exception {
    try (WriteOptions writeOpts = new WriteOptions()) {
      writeOpts.setDisableWAL(false);
      try (WriteBatch batch = new WriteBatch()) {
        preparePutEntries(rows, txnId, (k, v) -> batch.put(k.buffer(), v));
        db.write(writeOpts, batch);
      }
    }
  }

  @Override
  public void delete(final ByteArraySlice key) throws Exception {
    db.delete(key.buffer());
  }

  @Override
  public void deletePrefix(final ByteArraySlice keyPrefix) throws Exception {
    final byte[] prefix = keyPrefix.buffer();
    db.deleteRange(prefix, BytesUtil.prefixEndKey(prefix));
  }

  private long lastFlush = System.nanoTime();

  @Override
  public void flush() throws Exception {
    final long now = System.nanoTime();
    if ((now - lastFlush) < TimeUnit.HOURS.toNanos(1)) {
      Logger.debug("skipping requested flush on {} last flush was {}",
        getProjectId(), HumansUtil.humanTimeNanos(now - lastFlush));
      return;
    }

    try (FlushOptions opts = new FlushOptions()) {
      db.flush(opts);
    }
    Logger.debug("flush took {}", HumansUtil.humanTimeSince(now));
    lastFlush = now;
    compact();
  }

  @Override
  public void compact() throws Exception {
    final long startTime = System.nanoTime();
    db.compactRange();
    Logger.debug("{} compacted in {}", getProjectId(), HumansUtil.humanTimeSince(startTime));
  }

  // ================================================================================
  //  Scan Related
  // ================================================================================
	@Override
	protected Iterator<Entry<ByteArraySlice, byte[]>> scanPrefix(final ByteArraySlice prefix) {
    return new RocksPrefixIterator(db, prefix.buffer());
  }

  private static final class RocksPrefixIterator implements PeekIterator<Entry<ByteArraySlice, byte[]>> {
    private final RocksIterator iter;
    private final ReadOptions opts;
    private final byte[] prefix;

    private Map.Entry<ByteArraySlice, byte[]> nextItem;
    private boolean hasItem;
    private long scannedRows = 0;

    private RocksPrefixIterator(final RocksDB db, final byte[] prefix) {
      this.opts = new ReadOptions();
      this.iter = db.newIterator(opts);
      this.prefix = prefix;
      this.iter.seek(prefix);
      computeNext();
    }

    @Override
    public boolean hasNext() {
      return hasItem;
    }

    @Override
    public Entry<ByteArraySlice, byte[]> peek() {
      return nextItem;
    }

    @Override
    public Entry<ByteArraySlice, byte[]> next() {
      if (!hasItem) {
        throw new NoSuchElementException();
      }

      final Entry<ByteArraySlice, byte[]> value = nextItem;
      computeNext();
      return value;
    }

    private void computeNext() {
      while (iter.isValid()) {
        final byte[] key = iter.key();
        scannedRows++;

        if (SKIP_SYS_ROWS) {
          if (BytesUtil.hasPrefix(key, 0, key.length, AbstractKvStore.SYS_COUNTERS, 0, AbstractKvStore.SYS_COUNTERS.length)) {
            continue;
          }

          if (BytesUtil.hasPrefix(key, 0, key.length, AbstractKvStore.SYS_SCHEMAS, 0, AbstractKvStore.SYS_SCHEMAS.length)) {
            continue;
          }

          if (BytesUtil.hasPrefix(key, 0, key.length, EntitySchema.SYS_TXN_PREFIX.getBytes(), 0, EntitySchema.SYS_TXN_PREFIX.length())) {
            continue;
          }
        }

        if (BytesUtil.prefix(key, 0, key.length, prefix, 0, prefix.length) == prefix.length) {
          final byte[] value = iter.value();
          iter.next();
          this.hasItem = true;
          this.nextItem = Map.entry(new ByteArraySlice(key), value);
          return;
        } else {
          break;
        }
      }

      //System.out.println("SCANNED ROWS " + scannedRows);
      this.hasItem = false;
      this.nextItem = null;
      IOUtil.closeQuietly(this.iter);
      IOUtil.closeQuietly(this.opts);
    }
  }

  public static boolean SKIP_SYS_ROWS = false;
}
