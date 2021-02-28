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

package tech.dnaco.logging.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.logging.LogEntry;
import tech.dnaco.logging.LogEntry.LogEntryType;
import tech.dnaco.logging.LogEntryData;
import tech.dnaco.logging.LogEntryMessage;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public interface LogFormat {
  LogFormat[] VERSIONS = new LogFormat[] {
    new LogFormatV0()
  };
  LogFormat CURRENT = VERSIONS[0];

  int getVersion();

  void writeEntryMessage(PagedByteArray buffer, LogEntryMessage entry);
  void writeEntryData(PagedByteArray buffer, LogEntryData entry);

  LogEntryWriter newEntryWriter();
  LogEntryReader newEntryReader(final InputStream stream) throws IOException;

  interface LogEntryWriter extends AutoCloseable {
    void newBlock(OutputStream stream) throws IOException;
    void reset(OutputStream stream, Thread thread) throws IOException;
    void add(OutputStream stream, PagedByteArray buffer, int offset) throws IOException;
  }

  interface LogEntryReader extends AutoCloseable {
    void readResetEntry(InputStream stream) throws IOException;
    boolean fetchEntryHead(InputStream stream, LogEntryType type) throws IOException;
    LogEntry fetchEntryData(InputStream stream) throws IOException;
    void skipEntryData(InputStream stream) throws IOException;
    LogEntryHeader getEntryHeader();
  }

  final class LogEntryHeader {
    public String threadName;
    public LogEntryType type;
    public String module;
    public String owner;
    public long timestamp;
    public long traceIdHi;
    public long traceIdLo;
    public long spanId;

    public TraceId getTraceId() {
      return new TraceId(traceIdHi, traceIdLo);
    }

    public SpanId getSpanId() {
      return new SpanId(spanId);
    }
  }

  // ===============================================================================================
  //  Journal Entry Header
  // ===============================================================================================
  /*
   * Journal Entry Header
   *  type        off:0  len:1 byte
   *  timestamp   off:1  len:8 bytes
   *  traceId     off:9  len:8 bytes
   *  module      off:1  len:1 + N bytes
   *  owner       1 + N bytes
   */
  class LogJournalEntryHeader {
    public int type;

    public long timestamp;
    public long traceIdHi;
    public long traceIdLo;
    public long spanId;
    public byte[] module;
    public byte[] owner;
    public int dataOffset;
    public int dataLength;
  }

  static void writeJournalHeader(final PagedByteArray buffer, final LogEntry entry) {
    buffer.add(entry.getType().ordinal() & 0xff);
    buffer.addFixed64(entry.getTimestamp());
    buffer.addFixed64(entry.getTraceId().getHi());
    buffer.addFixed64(entry.getTraceId().getLo());
    buffer.addFixed64(entry.getSpanId().getSpanId());
    buffer.addBlob8(StringUtil.defaultIfEmpty(entry.getModule(), "unknown"));
    buffer.addBlob8(StringUtil.defaultIfEmpty(entry.getOwner(), "unknown"));
  }

  static LogJournalEntryHeader readJournalHeader(final PagedByteArray buffer, final int offset) {
    final LogJournalEntryHeader head = new LogJournalEntryHeader();
    head.type = buffer.get(offset);
    head.timestamp = buffer.getFixed64(offset + 1);
    head.traceIdHi = buffer.getFixed64(offset + 9);
    head.traceIdLo = buffer.getFixed64(offset + 17);
    head.spanId = buffer.getFixed64(offset + 25);
    head.module = buffer.getBlob8(offset + 33);
    head.owner = buffer.getBlob8(offset + 34 + head.module.length);
    head.dataOffset = offset + 35 + head.module.length + head.owner.length;
    head.dataLength = buffer.getFixed32(head.dataOffset);
    head.dataOffset += 4;
    return head;
  }
}
