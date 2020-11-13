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

package tech.dnaco.logging;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.logging.LogEntry.LogEntryType;

/*
 * | entry type | entry data |
 *
 * | RESET   | version | thread |
 * | TRACE   | timestamp | traceId | module | owner | thread | ... |
 * | MESSAGE | timestamp | traceId | module | owner | thread | ... |
 * | DATA    | timestamp | traceId | module | owner | thread | ... |
 */
public abstract class LogFormat {
  private static final LogFormat[] VERSIONS = new LogFormat[] {
    new LogFormatV0()
  };

  public static final LogFormat CURRENT = VERSIONS[0];

  public abstract int getVersion();

  public abstract void writeEntryData(PagedByteArray buffer, LogEntryMessage logEntryMessage);

  public abstract LogEntryWriter newEntryWriter();
  protected abstract LogEntryReader newEntryReader(final InputStream stream) throws IOException;

  interface LogEntryWriter extends AutoCloseable {
    void reset(OutputStream stream, Thread thread) throws IOException;
    void add(OutputStream stream, PagedByteArray buffer, int offset) throws IOException;
  }

  interface LogEntryReader extends AutoCloseable {
    boolean fetchEntryHead(InputStream stream) throws IOException;
    LogEntry fetchEntryData(InputStream stream) throws IOException;
    void skipEntryData(InputStream stream) throws IOException;
	  LogEntryHeader getEntryHeader();
  }

  private static final class LogFormatV0 extends LogFormat {
    @Override
    public int getVersion() {
      return 0;
    }

    @Override
    public LogEntryWriter newEntryWriter() {
      return new DeltaWriter();
    }

    @Override
    protected LogEntryReader newEntryReader(final InputStream stream) throws IOException {
      return new DeltaReader(stream);
    }

    @Override
    public void writeEntryData(final PagedByteArray buffer, final LogEntryMessage entry) {
      // level 1byte
      buffer.add((entry.hasException() ? 1 << 7 : 0) | (entry.getLevel().ordinal() & 0xff));

      // write class and method 1b + N
      LogSerde.writeBlob8(buffer, entry.getClassAndMethod());

      // write msg format
      LogSerde.writeString(buffer, entry.getMsgFormat());

      // write msg args
      if (entry.hasMsgArgs()) {
        final String[] args = entry.getMsgArgs();
        LogSerde.writeVarInt(buffer, args.length);

        for (int i = 0; i < args.length; ++i) {
          LogSerde.writeString(buffer, args[i]);
        }
      } else {
        buffer.add(0);
      }

      // write the exception
      if (entry.hasException()) {
        LogSerde.writeString(buffer, entry.getException());
      }
    }

    private void readEntryData(final LogEntryMessage entry, final InputStream stream) throws IOException {
      // read level (and flag has exception)
      final int level = stream.read();
      entry.setLevel(LogUtil.levelFromOrdinal(level & 0x7f));

      // class and method
      entry.setClassAndMethod(new String(LogSerde.readBlob8(stream), StandardCharsets.UTF_8));

      // msg format
      entry.setMsgFormat(LogSerde.readString(stream));

      // msg args
      final int argsLen = IntDecoder.LITTLE_ENDIAN.readUnsignedVarInt(stream);
      if (argsLen > 0) {
        final String[] args = new String[argsLen];
        for (int i = 0; i < argsLen; ++i) {
          args[i] = LogSerde.readString(stream);
        }
        entry.setMsgArgs(args);
      } else {
        entry.setMsgArgs(null);
      }

      // read exception
      if ((level & (1 << 7)) != 0) {
        entry.setException(LogSerde.readString(stream));
      } else {
        entry.setException(null);
      }
    }

    private final class DeltaReader implements LogEntryReader {
      private final String threadName;

      private LogEntryType type;
      private String lastModule;
      private String lastOwner;
      private long lastTimestamp;
      private long lastTraceId;
      private int dataLength;

      public DeltaReader(final InputStream stream) throws IOException {
        this.threadName = LogSerde.readString(stream);
      }

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
        header.traceId = lastTraceId;
        return header;
      }

      @Override
      public boolean fetchEntryHead(final InputStream stream) throws IOException {
        // | type 1b | delta-ts vint | traceId vint | module-len 1b | owner-len 1b | data-len vint |
        // | module Nb | owner Nb | data Nb |

        // read type (1byte)
        final int vType = stream.read();
        if (vType < 0) throw new EOFException();

        this.type = LogEntryType.values()[vType];
        if (this.type == LogEntryType.RESET) return false;

        // read delta-timestamp (vint)
        this.lastTimestamp += IntDecoder.LITTLE_ENDIAN.readUnsignedVarLong(stream);

        // read traceId or 0 (vint)
        final long xTraceId = IntDecoder.LITTLE_ENDIAN.readUnsignedVarLong(stream);
        if (xTraceId != 0) this.lastTraceId = xTraceId;

        // write | module len | owner len | data len |
        final int moduleLength = stream.read() & 0xff;
        final int ownerLength = stream.read() & 0xff;
        this.dataLength = IntDecoder.LITTLE_ENDIAN.readUnsignedVarInt(stream);

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
            readEntryData((LogEntryMessage) entry, stream);
            break;
          case TRACE:
            entry = null;
            break;
          case RESET:
            entry = null;
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
          entry.setTraceId(lastTraceId);
        }
        return entry;
      }

      @Override
      public void skipEntryData(final InputStream stream) throws IOException {
        stream.skip(dataLength);
      }
    }

    private static final class DeltaWriter implements LogEntryWriter {
      private byte[] lastModule;
      private byte[] lastOwner;
      private long lastTimestamp;
      private long lastTraceId;

      @Override
      public void close() {
        // no-op
      }

      @Override
      public void reset(final OutputStream stream, final Thread thread) throws IOException {
        // write: | reset | version | thread |
        stream.write(LogEntryType.RESET.ordinal());
        stream.write(LogFormat.CURRENT.getVersion() & 0xff);
        LogSerde.writeString(stream, thread.getName());

        this.lastModule = null;
        this.lastOwner = null;
        this.lastTimestamp = 0;
        this.lastTraceId = 0;
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
        IntEncoder.LITTLE_ENDIAN.writeUnsignedVarLong(stream, head.timestamp - lastTimestamp);
        this.lastTimestamp = head.timestamp;

        // write traceId or 0 (vint)
        IntEncoder.LITTLE_ENDIAN.writeUnsignedVarLong(stream, head.traceId == lastTraceId ? 0 : head.traceId);
        this.lastTraceId = head.traceId;

        // write | module len | owner len | data len |
        stream.write(sameModule ? 0 : head.module.length);
        stream.write(sameOwner ? 0 : head.owner.length);
        IntEncoder.LITTLE_ENDIAN.writeUnsignedVarLong(stream, head.dataLength);

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
  }

  /*
   * Journal Entry Header
   *  type        off:0  len:1 byte
   *  timestamp   off:1  len:8 bytes
   *  traceId     off:9  len:8 bytes
   *  module      off:1  len:1 + N bytes
   *  owner       1 + N bytes
   */
  public static class LogJournalEntryHeader {
    public int type;
    public long timestamp;
    public long traceId;
    public byte[] module;
    public byte[] owner;
    public int dataOffset;
    public int dataLength;
  }

  public static void writeJournalHeader(final PagedByteArray buffer, final LogEntry entry) {
    buffer.add(entry.getType().ordinal() & 0xff);
    buffer.addFixed64(entry.getTimestamp());
    buffer.addFixed64(entry.getTraceId());
    LogSerde.writeBlob8(buffer, entry.getModule());
    LogSerde.writeBlob8(buffer, entry.getOwner());
  }

  public static LogJournalEntryHeader readJournalHeader(final PagedByteArray buffer, final int offset) {
    final LogJournalEntryHeader head = new LogJournalEntryHeader();
    head.type = buffer.get(offset);
    head.timestamp = buffer.getFixed64(offset + 1);
    head.traceId = buffer.getFixed64(offset + 9);
    head.module = LogSerde.readBlob8(buffer, offset + 17);
    head.owner = LogSerde.readBlob8(buffer, offset + 18 + head.module.length);
    head.dataOffset = offset + 19 + head.module.length + head.owner.length;
    head.dataLength = buffer.getFixed32(head.dataOffset);
    head.dataOffset += 4;
    return head;
  }

  public static LogReader newReader(final InputStream stream) throws IOException {
    return new LogReader(stream);
  }

  public static final class LogEntryHeader {
    public String threadName;
    public LogEntryType type;
    public String module;
    public String owner;
    public long timestamp;
    public long traceId;
  }

  public static final class LogReader {
    private final InputStream stream;
    private LogEntryReader reader;

    public LogReader(final InputStream stream) throws IOException {
      this.stream = stream;

      final int type = stream.read();
      if (type < 0) throw new EOFException();
      if ((type & 0xff) != LogEntryType.RESET.ordinal()) {
        throw new IOException("expected log entry type RESET");
      }
      nextReader();
    }

    public LogEntryHeader getEntryHeader() {
      return reader.getEntryHeader();
    }

    public boolean readEntryHead() throws IOException {
      if (reader.fetchEntryHead(stream)) return true;

      do {
        nextReader();
      } while (!reader.fetchEntryHead(stream));
      return true;
    }

    public LogEntry readEntryData() throws IOException {
      return reader.fetchEntryData(stream);
    }

    public void skipEntryData() throws IOException {
      reader.skipEntryData(stream);
    }

    private void nextReader() throws IOException {
      final int version = stream.read() & 0xff;
      reader = VERSIONS[version].newEntryReader(stream);
    }
  }
}
