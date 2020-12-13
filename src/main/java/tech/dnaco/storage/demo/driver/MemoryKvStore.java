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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;

public class MemoryKvStore extends AbstractKvStore {
  private final ConcurrentSkipListMap<ByteArraySlice, byte[]> rows = new ConcurrentSkipListMap<>();

  public MemoryKvStore(final String projectId) {
		super(projectId);
	}

  public void dump() {
    System.out.println(" --- STORAGE " + rows.size() + " ---");
    for (final Entry<ByteArraySlice, byte[]> entry: rows.entrySet()) {
      System.out.println(" -> " + entry.getKey());
    }
  }

  // ================================================================================
  //  Init Related
  // ================================================================================
  @Override
  protected void openKvStore() throws Exception {
    // no-op
  }

  @Override
  protected void shutdownKvStore() {
    // no-op
  }

  // ================================================================================
  //  Put Related
  // ================================================================================
  @Override
  public void put(final ByteArraySlice key, final byte[] value) {
    this.rows.put(key, value);
  }

  @Override
  public void delete(final ByteArraySlice key) throws Exception {
    this.rows.remove(key);
  }

  @Override
  public void deletePrefix(final ByteArraySlice keyPrefix) throws Exception {
    final Iterator<ByteArraySlice> it = rows.subMap(keyPrefix, prefixEndKey(keyPrefix)).keySet().iterator();
    while (it.hasNext()) {
      it.next();
      it.remove();
    }
  }

  // ================================================================================
  //  Scan Related
  // ================================================================================
  public Iterator<EntityDataRow> scan(final String groupId) throws Exception {
    return new RowIterator(scanPrefix(new RowKeyBuilder().add(groupId).addKeySeparator().slice()));
  }

  @Override
  protected Iterator<Entry<ByteArraySlice, byte[]>> scanPrefix(final ByteArraySlice prefix) throws Exception {
    return rows.subMap(prefix, prefixEndKey(prefix)).entrySet().iterator();
    //final Iterator<Entry<ByteArraySlice, byte[]>> itEntry = rows.tailMap(prefix).entrySet().iterator();
    //return new FilteredIterator<>(itEntry, (entry) -> hasPrefix(entry.getKey(), prefix));
  }

  private static boolean hasPrefix(final ByteArraySlice key, final ByteArraySlice prefix) {
    return BytesUtil.hasPrefix(key.rawBuffer(), key.offset(), key.length(), prefix.rawBuffer(), prefix.offset(), prefix.length());
  }
}
