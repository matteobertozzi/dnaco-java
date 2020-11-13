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

package tech.dnaco.journal;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import tech.dnaco.collections.paged.PagedByteArray;

public class JournalBuffer {
  private final HashMap<String, LogGroup> tenants = new HashMap<>(64);
  private final PagedByteArray buffer = new PagedByteArray(1 << 20);
  private final Thread thread;

  private LogGroup lastTenant = null;

  public JournalBuffer() {
    this.thread = Thread.currentThread();
  }

  public Thread getThread() {
    return thread;
  }

  public Collection<LogGroup> getGroups() {
    return tenants.values();
  }

  public Set<String> getTenantIds() {
    return tenants.keySet();
  }

  public int size() {
    return buffer.size();
  }

  public int add(final JournalEntry entry) {
    final LogGroup group = computeIfAbsentLogGroup(entry.getTenantId());
    group.add(buffer, entry);
    return buffer.size();
  }

  public void process(final String tenantId, final LogBufferEntryProcessor processor)
      throws IOException {
    final LogGroup group = tenants.get(tenantId);
    if (group != null) group.process(buffer, processor);
  }

  public boolean hasTenantId(final String tenantId) {
    return tenants.containsKey(tenantId);
  }

  private LogGroup computeIfAbsentLogGroup(final String tenantId) {
    if (lastTenant != null && lastTenant.getTenantId().equals(tenantId)) {
      return lastTenant;
    }
    lastTenant = tenants.computeIfAbsent(tenantId, (k) -> new LogGroup(tenantId));
    return lastTenant;
  }

  public static final class LogGroup {
    private static final int ENTRY_OFFSET_EOF = 0xffffffff;

    private final String tenantId;
    private int head = -1;
    private int tail = -1;

    private LogGroup(final String tenantId) {
      this.tenantId = tenantId;
    }

    public String getTenantId() {
      return tenantId;
    }

    protected void add(final PagedByteArray buffer, final JournalEntry entry) {
      final int offset = buffer.size();

      // update the previous entry with the next (offset) pointer
      if (this.tail < 0) {
        this.head = offset;
      } else {
        buffer.setFixed32(tail, offset);
      }
      this.tail = offset;

      // write the new entry: | u32 next (EOF) | entry data... |
      buffer.addFixed32(ENTRY_OFFSET_EOF);
      entry.write(buffer);
    }

    public void process(final PagedByteArray buffer, final LogBufferEntryProcessor processor)
        throws IOException {
      int lastOffset = head;
      int offset = head;
      while (offset != ENTRY_OFFSET_EOF) {
        lastOffset = offset;

        final int next = buffer.getFixed32(offset);
        processor.process(buffer, offset + 4);

        offset = next;
      }

      if (lastOffset != tail) {
        System.err.println("unexpected tail: lastOffset=" + lastOffset + " tail=" + tail + " offset=" + offset);
      }
    }
  }

  public interface LogBufferEntryProcessor {
    void process(PagedByteArray buffer, int offset) throws IOException;
  }
}
