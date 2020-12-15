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

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.LogEntry.LogEntryType;
import tech.dnaco.strings.StringUtil;

/*
 * | entry type | entry data |
 *
 * | BLOCK   | magic |
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

  public abstract void writeEntryMessage(PagedByteArray buffer, LogEntryMessage logEntryMessage);
  public abstract void writeEntryTask(PagedByteArray buffer, LogEntryTask logEntryTask);

  public abstract LogEntryWriter newEntryWriter();
  protected abstract LogEntryReader newEntryReader(final InputStream stream) throws IOException;

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

  private static final class LogFormatV0 extends LogFormat {
    private static final byte[] FLUSH_MAGIC = new byte[] {
      (byte) 0x5a, (byte) 0xd9, (byte) 0x2f, (byte) 0xd6, (byte) 0x41, (byte) 0xf0, (byte) 0x20, (byte) 0xb1,
      (byte) 0x08, (byte) 0xf0, (byte) 0x9c, (byte) 0x1b, (byte) 0xb1, (byte) 0x31, (byte) 0x07, (byte) 0x2b
    };

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
      // read magic
      final byte[] magic = new byte[FLUSH_MAGIC.length];
      IOUtil.readNBytes(stream, magic, 0, magic.length);

      // check magic
      //System.out.println("MAGIC " + Arrays.toString(magic) + " -> " + Arrays.toString(FLUSH_MAGIC) + " -> " + BytesUtil.equals(FLUSH_MAGIC, magic));
      if (!BytesUtil.equals(FLUSH_MAGIC, magic)) return null;

      return new DeltaReader();
    }

    @Override
    public void writeEntryMessage(final PagedByteArray buffer, final LogEntryMessage entry) {
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

    private void readEntryMessage(final LogEntryMessage entry, final InputStream stream) throws IOException {
      // read level (and flag has exception)
      final int level = stream.read();
      entry.setLevel(LogUtil.levelFromOrdinal(level & 0x7f));

      // class and method
      entry.setClassAndMethod(new String(LogSerde.readBlob8(stream), StandardCharsets.UTF_8));

      // msg format
      entry.setMsgFormat(LogSerde.readString(stream));

      // msg args
      final int argsLen = LogSerde.readVarInt(stream);
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


    @Override
    public void writeEntryTask(final PagedByteArray buffer, final LogEntryTask entry) {
      final boolean state = entry.isComplete();

      // state 1byte
      buffer.add(state ? 1 : 0);

      // write parentId
      LogSerde.writeVarInt(buffer, entry.getParentId());

      // write class and method 1b + N
      LogSerde.writeBlob8(buffer, entry.getCallerMethodAndLine());

      // write label
      LogSerde.writeString(buffer, entry.getLabel());

      // write elapsedNs
      if (state) {
        LogSerde.writeVarInt(buffer, entry.getElapsedNs());
      }

      // write key/vals
      LogSerde.writeVarInt(buffer, entry.getAttributesCount());
      for (int i = 0, n = entry.getAttributesCount(); i < n; ++i) {
        LogSerde.writeString(buffer, entry.getAttributeKey(i));
        LogSerde.writeString(buffer, entry.getAttributeValue(i));
      }
    }

    private void readEntryTask(final LogEntryTask entry, final InputStream stream) throws IOException {
      final int state = stream.read() & 0xff;

      // read parentId
      entry.setParentId(LogSerde.readVarInt(stream));

      // read class and method
      final byte[] methodAndLine = LogSerde.readBlob8(stream);
      if (methodAndLine != null) {
        entry.setCallerMethodAndLine(new String(methodAndLine, StandardCharsets.UTF_8));
      }

      // read label
      entry.setLabel(LogSerde.readString(stream));

      if (state == 1) {
        entry.setElapsedNs(LogSerde.readVarInt(stream));
      }

      // read key/vals
      final int attributesCount = LogSerde.readVarInt(stream);
      if (attributesCount > 0) {
        final String[] keys = new String[attributesCount];
        final String[] vals = new String[attributesCount];
        for (int i = 0; i < attributesCount; ++i) {
          keys[i] = LogSerde.readString(stream);
          vals[i] = LogSerde.readString(stream);
        }
        entry.setAttributes(keys, vals);
      }
    }

    private final class DeltaReader implements LogEntryReader {
      private String threadName;
      private LogEntryType type;
      private String lastModule;
      private String lastOwner;
      private long lastTimestamp;
      private long lastTraceId;
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
        header.traceId = lastTraceId;
        return header;
      }

      public void readResetEntry(final InputStream stream) throws IOException {
        this.threadName = LogSerde.readString(stream);
        this.lastModule = null;
        this.lastOwner = null;
        this.lastTimestamp = 0;
        this.lastTraceId = 0;
      }

      @Override
      public boolean fetchEntryHead(final InputStream stream, final LogEntryType type) throws IOException {
        // | type 1b | delta-ts vint | traceId vint | module-len 1b | owner-len 1b | data-len vint |
        // | module Nb | owner Nb | data Nb |
        this.type = type;

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
            readEntryMessage((LogEntryMessage) entry, stream);
            break;
          case TASK:
            entry = new LogEntryTask();
            readEntryTask((LogEntryTask) entry, stream);
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

    private final class DeltaWriter implements LogEntryWriter {
      private byte[] lastModule;
      private byte[] lastOwner;
      private long lastTimestamp;
      private long lastTraceId;

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
    LogSerde.writeBlob8(buffer, StringUtil.defaultIfEmpty(entry.getModule(), "unknown"));
    LogSerde.writeBlob8(buffer, StringUtil.defaultIfEmpty(entry.getOwner(), "unknown"));
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
      nextReader();
    }

    public LogEntryHeader getEntryHeader() {
      return reader.getEntryHeader();
    }

    public boolean readEntryHead() throws IOException {
      while (true) {
        try {
          // read type (1byte)
          final int vType = stream.read();
          if (vType < 0) throw new EOFException();

          final LogEntryType type = LogEntryType.values()[vType];
          //System.out.println(" -> TYPE: " + type);
          switch (type) {
            case FLUSH:
              if (!readFlushEntry()) {
                nextReader();
              }
              break;
            case RESET:
              reader.readResetEntry(stream);
              break;
            default:
              if (reader.fetchEntryHead(stream, type)) {
                return true;
              }
              nextReader();
              break;
          }
        } catch (final EOFException e) {
          throw e;
        } catch (final IOException e) {
          Logger.error("failed to read entries. skipping blocks: {}", e.getMessage());
          nextReader();
        }
      }
    }

    public LogEntry readEntryData() throws IOException {
      return reader.fetchEntryData(stream);
    }

    public void skipEntryData() throws IOException {
      reader.skipEntryData(stream);
    }

    private void nextReader() throws IOException {
      while (true) {
        final int type = stream.read();
        if (type < 0) throw new EOFException();
        if (type != LogEntryType.FLUSH.ordinal()) continue;
        //System.out.println("READ FLUSH TYPE " + type);
        if (readFlushEntry()) break;
      }
    }

    private boolean readFlushEntry() throws IOException {
      // FLUSH entry block start with a version field
      final int version = stream.read();
      if (version < 0) throw new EOFException();

      // invalid version
      //System.out.println("READ FLUSH VERSION " + version);
      if (version >= VERSIONS.length) return false;

      reader = VERSIONS[version].newEntryReader(stream);
      return reader != null;
    }
  }
}
