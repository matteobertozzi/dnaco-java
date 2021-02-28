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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.LogEntry;
import tech.dnaco.logging.LogEntry.LogEntryType;
import tech.dnaco.logging.LogEntryData;
import tech.dnaco.logging.LogEntryMessage;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public class LogFormatV0 implements LogFormat {
  private static final byte[] FLUSH_MAGIC = new byte[] {
    (byte) 0x5a, (byte) 0xd9, (byte) 0x2f, (byte) 0xd6, (byte) 0x41, (byte) 0xf0, (byte) 0x20, (byte) 0xb1,
    (byte) 0x08, (byte) 0xf0, (byte) 0x9c, (byte) 0x1b, (byte) 0xb1, (byte) 0x31, (byte) 0x07, (byte) 0x2b
  };

  @Override
  public int getVersion() {
    return 0;
  }

  // ===============================================================================================
  //  LogEntry related
  // ===============================================================================================
  public static void writeVarInt(final PagedByteArray buffer, final long value) {
    final byte[] buf8 = new byte[9];
    final int vlen = IntEncoder.writeUnsignedVarLong(buf8, 0, value);
    buffer.add(buf8, 0, vlen);
  }

  public static void writeString(final PagedByteArray buffer, final String value) {
    if (StringUtil.isEmpty(value)) {
      buffer.add(0);
    } else {
      final byte[] blob = value.getBytes(StandardCharsets.UTF_8);
      writeVarInt(buffer, blob.length);
      buffer.add(blob);
    }
  }

  @Override
  public void writeEntryMessage(final PagedByteArray buffer, final LogEntryMessage entry) {
    // level 1byte
    buffer.add((entry.hasException() ? 1 << 7 : 0) | (entry.getLevel().ordinal() & 0xff));

    // write class and method 1b + N
    buffer.addBlob8(entry.getClassAndMethod());

    // write msg format
    writeString(buffer, entry.getMsgFormat());

    // write msg args
    if (entry.hasMsgArgs()) {
      final String[] args = entry.getMsgArgs();
      writeVarInt(buffer, args.length);

      for (int i = 0; i < args.length; ++i) {
        writeString(buffer, args[i]);
      }
    } else {
      buffer.add(0);
    }

    // write the exception
    if (entry.hasException()) {
      writeString(buffer, entry.getException());
    }
  }

  private void readEntryMessage(final LogEntryMessage entry, final InputStream stream) throws IOException {
    // read level (and flag has exception)
    final int level = stream.read();
    entry.setLevel(LogUtil.levelFromOrdinal(level & 0x7f));

    // class and method
    entry.setClassAndMethod(new String(IOUtil.readBlob8(stream)));

    // msg format
    entry.setMsgFormat(IOUtil.readString(stream));

    // msg args
    final int argsLen = IntDecoder.readUnsignedVarInt(stream);
    if (argsLen > 0) {
      final String[] args = new String[argsLen];
      for (int i = 0; i < argsLen; ++i) {
        args[i] = IOUtil.readString(stream);
      }
      entry.setMsgArgs(args);
    } else {
      entry.setMsgArgs(null);
    }

    // read exception
    if ((level & (1 << 7)) != 0) {
      entry.setException(IOUtil.readString(stream));
    } else {
      entry.setException((String)null);
    }
  }

  @Override
  public void writeEntryData(final PagedByteArray buffer, final LogEntryData entry) {
    // TODO Auto-generated method stub

  }

  // ===============================================================================================
  //  Writer related
  // ===============================================================================================
  @Override
  public LogEntryWriter newEntryWriter() {
    return new DeltaWriter();
  }

  private final class DeltaWriter implements LogEntryWriter {
    private byte[] lastModule;
    private byte[] lastOwner;
    private long lastTimestamp;
    private long lastTraceIdHi;
    private long lastTraceIdLo;
    private long lastSpanId;

    @Override
    public void close() {
      // no-op
    }

    @Override
    public void newBlock(final OutputStream stream) throws IOException {
      stream.write(LogEntryType.FLUSH.ordinal());
      stream.write(getVersion() & 0xff);
      stream.write(FLUSH_MAGIC);
    }

    @Override
    public void reset(final OutputStream stream, final Thread thread) throws IOException {
      // write: | reset | version | thread |
      stream.write(LogEntryType.RESET.ordinal());
      IOUtil.writeString(stream, thread.getName());

      this.lastModule = null;
      this.lastOwner = null;
      this.lastTimestamp = 0;
      this.lastTraceIdHi = 0;
      this.lastTraceIdLo = 0;
      this.lastSpanId = 0;
    }

    @Override
    public void add(final OutputStream stream, final PagedByteArray entryBuffer, final int entryOffset)
        throws IOException {
      final LogJournalEntryHeader head = LogFormat.readJournalHeader(entryBuffer, entryOffset);

      final boolean sameModule = Arrays.equals(head.module, lastModule);
      final boolean sameOwner = Arrays.equals(head.owner, lastOwner);

      // | type 1b | delta-ts vint | traceId vint | module-len 1b | owner-len 1b | data-len vint |
      // | module Nb | owner Nb | data Nb |

      // write type (1byte)
      stream.write(head.type);

      // write delta-timestamp (vint)
      IntEncoder.writeUnsignedVarLong(stream, head.timestamp - lastTimestamp);
      this.lastTimestamp = head.timestamp;

      // write traceId or 0 (vint)
      IntEncoder.writeUnsignedVarLong(stream, head.traceIdHi == lastTraceIdHi ? 0 : head.traceIdHi);
      this.lastTraceIdHi = head.traceIdHi;
      IntEncoder.writeUnsignedVarLong(stream, head.traceIdLo == lastTraceIdLo ? 0 : head.traceIdLo);
      this.lastTraceIdLo = head.traceIdLo;
      IntEncoder.writeUnsignedVarLong(stream, head.spanId == lastSpanId ? 0 : head.spanId);
      this.lastSpanId = head.spanId;

      // write | module len | owner len | data len |
      stream.write(sameModule ? 0 : head.module.length);
      stream.write(sameOwner ? 0 : head.owner.length);
      IntEncoder.writeUnsignedVarLong(stream, head.dataLength);

      // write module
      if (!sameModule) {
        stream.write(head.module, 0, head.module.length);
        this.lastModule = head.module;
      }

      // write owner
      if (!sameOwner) {
        stream.write(head.owner, 0, head.owner.length);
        this.lastOwner = head.owner;
      }

      // write data
      entryBuffer.forEach(head.dataOffset, head.dataLength, stream::write);
    }
  }

  // ===============================================================================================
  //  Reader related
  // ===============================================================================================
  @Override
  public LogEntryReader newEntryReader(final InputStream stream) throws IOException {
    // read magic
    final byte[] magic = new byte[FLUSH_MAGIC.length];
    IOUtil.readNBytes(stream, magic, 0, magic.length);

    // check magic
    //System.out.println("MAGIC " + Arrays.toString(magic) + " -> " + Arrays.toString(FLUSH_MAGIC) + " -> " + BytesUtil.equals(FLUSH_MAGIC, magic));
    if (!BytesUtil.equals(FLUSH_MAGIC, magic)) return null;

    return new DeltaReader();
  }

  private final class DeltaReader implements LogEntryReader {
    private String threadName;
    private LogEntryType type;
    private String lastModule;
    private String lastOwner;
    private long lastTimestamp;
    private long lastTraceIdHi;
    private long lastTraceIdLo;
    private long lastSpanId;
    private int dataLength;

    @Override
    public void close() {
      // no-op
    }

    @Override
    public LogEntryHeader getEntryHeader() {
      final LogEntryHeader header = new LogEntryHeader();
      header.threadName = threadName;
      header.module = lastModule;
      header.owner = lastOwner;
      header.timestamp = lastTimestamp;
      header.traceIdHi = lastTraceIdHi;
      header.traceIdLo = lastTraceIdLo;
      header.spanId = lastSpanId;
      return header;
    }

    public void readResetEntry(final InputStream stream) throws IOException {
      this.threadName = IOUtil.readString(stream);
      this.lastModule = null;
      this.lastOwner = null;
      this.lastTimestamp = 0;
      this.lastTraceIdHi = 0;
      this.lastTraceIdLo = 0;
      this.lastSpanId = 0;
    }

    @Override
    public boolean fetchEntryHead(final InputStream stream, final LogEntryType type) throws IOException {
      // | type 1b | delta-ts vint | traceId/spanId 3vint | module-len 1b | owner-len 1b | data-len vint |
      // | module Nb | owner Nb | data Nb |
      this.type = type;

      // read delta-timestamp (vint)
      this.lastTimestamp += IntDecoder.readUnsignedVarLong(stream);

      // read traceId or 0 (vint)
      long xTraceId = IntDecoder.readUnsignedVarLong(stream);
      if (xTraceId != 0) this.lastTraceIdHi = xTraceId;
      xTraceId = IntDecoder.readUnsignedVarLong(stream);
      if (xTraceId != 0) this.lastTraceIdLo = xTraceId;
      xTraceId = IntDecoder.readUnsignedVarLong(stream);
      if (xTraceId != 0) this.lastSpanId = xTraceId;

      // write | module len | owner len | data len |
      final int moduleLength = stream.read() & 0xff;
      final int ownerLength = stream.read() & 0xff;
      this.dataLength = IntDecoder.readUnsignedVarInt(stream);

      // read module
      if (moduleLength > 0) {
        this.lastModule = new String(stream.readNBytes(moduleLength), StandardCharsets.UTF_8);
      }

      // write owner
      if (ownerLength > 0) {
        this.lastOwner = new String(stream.readNBytes(ownerLength), StandardCharsets.UTF_8);
      }

      return true;
    }

    @Override
    public LogEntry fetchEntryData(final InputStream stream) throws IOException {
      final LogEntry entry;
      switch (type) {
        case DATA:
          entry = null;
          break;
        case MESSAGE:
          entry = new LogEntryMessage();
          readEntryMessage((LogEntryMessage) entry, stream);
          break;
        default:
          throw new UnsupportedOperationException(type.name());
      }

      if (entry != null) {
        entry.setTenantId(null);
        entry.setModule(lastModule);
        entry.setOwner(lastOwner);
        entry.setThread(threadName);
        entry.setTimestamp(lastTimestamp);
        entry.setTraceId(new TraceId(lastTraceIdHi, lastTraceIdLo));
        entry.setSpanId(new SpanId(lastSpanId));
      }
      return entry;
    }

    @Override
    public void skipEntryData(final InputStream stream) throws IOException {
      stream.skip(dataLength);
    }
  }
}
