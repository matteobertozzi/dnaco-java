/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.logging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;

import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.io.BytesInputStream;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringFormat;
import tech.dnaco.strings.StringUtil;

public class LogEntry {
  public static final DateTimeFormatter LOG_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private String moduleId;
  private String groupId;
  private String threadName;
  private String classAndMethod;
  private String stackTrace;
  private String format;
  private Object[] args;
  private LogLevel level;
  private long timestamp;
  private long traceId;

  public void reset() {
    this.moduleId = null;
    this.groupId = null;
    this.threadName = null;
    this.classAndMethod = null;
    this.stackTrace = null;
    this.format = null;
    this.args = null;
    this.level = LogLevel.TRACE;
    this.timestamp = 0;
    this.traceId = 0;
  }

  public LogEntry setModuleId(final String moduleId) {
    this.moduleId = moduleId;
    return this;
  }

  public String getModuleId() {
    return moduleId;
  }

  public String getGroupId() {
    return groupId;
  }

  public LogEntry setGroupId(final String groupId) {
    this.groupId = groupId;
    return this;
  }

  public String getThreadName() {
    return threadName;
  }

  public LogEntry setThreadName(final String threadName) {
    this.threadName = threadName;
    return this;
  }

  public String getClassAndMethod() {
    return classAndMethod;
  }

  public LogEntry setClassAndMethod(final String classAndMethod) {
    this.classAndMethod = classAndMethod;
    return this;
  }

  public boolean hasStackTrace() {
    return StringUtil.isNotEmpty(stackTrace);
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public LogEntry setStackTrace(final String stackTrace) {
    this.stackTrace = stackTrace;
    return this;
  }

  public String getFormat() {
    return format;
  }

  public Object[] getFormatArgs() {
    return args;
  }

  public LogEntry setFormat(final String format, final Object[] args) {
    this.format = format;
    this.args = args;
    return this;
  }

  public LogLevel getLevel() {
    return level;
  }

  public LogEntry setLevel(final LogLevel level) {
    this.level = level;
    return this;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public LogEntry setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public long getTraceId() {
    return traceId;
  }

  public LogEntry setTraceId(final long traceId) {
    this.traceId = traceId;
    return this;
  }

  public static long copyTo(final BytesInputStream reader, final boolean binaryFormat, final OutputStream stream)
      throws IOException {
    final int blockLen = IntDecoder.BIG_ENDIAN.readFixed32(reader);
    if (binaryFormat) {
      IntEncoder.BIG_ENDIAN.writeFixed32(stream, blockLen);
    }
    reader.copyTo(blockLen, stream);
    return blockLen + (binaryFormat ? 4 : 0);
  }

  public void writeBlockTo(final PagedByteArray writer, final boolean binaryFormat) {
    final PagedByteArray block = new PagedByteArray(4 << 10);
    if (binaryFormat) {
      writeBinary(block);
    } else {
      writeText(block);
    }

    final byte[] buf4 = new byte[4];
    IntEncoder.BIG_ENDIAN.writeFixed32(buf4, 0, block.size());
    writer.add(buf4, 0, 4);
    block.forEach((buf, off, len) -> writer.add(buf, off, len));
  }

  // ===============================================================================================
  // Logger Text Format helpers
  // ===============================================================================================
  public void writeText(final PagedByteArray blob) {
    blob.add(LOG_DATE_FORMAT.format(HumansUtil.localFromEpochMillis(timestamp)).getBytes());
    blob.add(new byte[] { ' ', '[' });
    blob.add(LogUtil.toTraceId(traceId).getBytes());
    blob.add(':');
    blob.add(StringUtil.emptyIfNull(moduleId).getBytes());
    blob.add(':');
    blob.add(StringUtil.emptyIfNull(groupId).getBytes());
    blob.add(':');
    blob.add(threadName.getBytes());
    blob.add(new byte[] { ']', ' ' });
    blob.add(level.name().getBytes());
    blob.add(' ');
    blob.add(classAndMethod.getBytes());
    blob.add(new byte[] { ' ', '-', ' ' });
    if (args == null) {
      blob.add(format.getBytes());
    } else {
      blob.add(StringFormat.format(format, args).getBytes());
    }
    blob.add('\n');
    if (hasStackTrace()) {
      blob.add(stackTrace.getBytes());
      blob.add('\n');
    }
  }

  public StringBuilder printEntry(final String projectId, final StringBuilder builder) {
    builder.append(LOG_DATE_FORMAT.format(HumansUtil.localFromEpochMillis(getTimestamp())));
    builder.append(" [");
    builder.append(LogUtil.toTraceId(getTraceId()));
    builder.append(':');
    builder.append(StringUtil.emptyIfNull(moduleId));
    builder.append(':');
    builder.append(StringUtil.emptyIfNull(groupId));
    builder.append(':');
    builder.append(getThreadName());
    builder.append("] ");
    builder.append(getLevel().name());
    builder.append(' ');
    builder.append(getClassAndMethod());
    builder.append(" - ");
    StringFormat.applyFormat(builder, getFormat(), getFormatArgs());
    if (hasStackTrace()) {
      builder.append('\n');
      builder.append(getStackTrace());
    }
    return builder;
  }

  public void printEntry(final String projectId, final PrintStream stream) {
    stream.println(printEntry(projectId, new StringBuilder(1024)).toString());
  }

  // ===============================================================================================
  // Logger Text Format helpers
  // ===============================================================================================
  private static void writeBinaryFixed(final PagedByteArray blob, final byte[] buffer, final int bytesWidth,
      final long v) {
    IntEncoder.BIG_ENDIAN.writeFixed(buffer, 0, v, bytesWidth);
    blob.add(buffer, 0, bytesWidth);
  }

  private static void writeBinaryVarInt(final PagedByteArray blob, final byte[] buffer, final long v) {
    final int n = IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(buffer, 0, v);
    blob.add(buffer, 0, n);
  }

  private static void writeBinaryString(final PagedByteArray blob, final byte[] buffer, final String v) {
    if (StringUtil.isEmpty(v)) {
      blob.add(0);
    } else {
      final byte[] bytes = v.getBytes();
      writeBinaryVarInt(blob, buffer, bytes.length);
      blob.add(bytes, 0, bytes.length);
    }
  }

  private static String readBinaryString(final InputStream stream) throws IOException {
    final int length = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    if (length <= 0)
      return null;

    final byte[] buf = IOUtil.readNBytes(stream, length);
    return new String(buf);
  }

  public void writeBinary(final PagedByteArray blob) {
    final byte[] buffer = new byte[16];

    writeBinaryFixed(blob, buffer, 8, timestamp);
    writeBinaryFixed(blob, buffer, 8, traceId);
    blob.add(level.ordinal());
    writeBinaryString(blob, buffer, moduleId);
    writeBinaryString(blob, buffer, groupId);
    writeBinaryString(blob, buffer, threadName);
    writeBinaryString(blob, buffer, classAndMethod);

    writeBinaryString(blob, buffer, format);
    if (ArrayUtil.isEmpty(args)) {
      blob.add(0);
    } else {
      writeBinaryVarInt(blob, buffer, args.length);
      for (int i = 0, n = args.length; i < n; ++i) {
        writeBinaryString(blob, buffer, String.valueOf(args[i]));
      }
    }

    writeBinaryString(blob, buffer, stackTrace);
  }

  public boolean readBinary(final InputStream stream, final Predicate<LogEntry> headerPredicate) throws IOException {
    final int entryLength = IntDecoder.BIG_ENDIAN.readFixed32(stream);
    this.reset();

    this.timestamp = IntDecoder.BIG_ENDIAN.readFixed(stream, 8);
    this.traceId = IntDecoder.BIG_ENDIAN.readFixed(stream, 8);
    this.level = LogUtil.levelFromOrdinal(stream.read() & 0xff);

    if (headerPredicate != null && !headerPredicate.test(this)) {
      stream.skip(entryLength - 17);
      return false;
    }

    this.moduleId = readBinaryString(stream);
    this.groupId = readBinaryString(stream);
    this.threadName = readBinaryString(stream);
    this.classAndMethod = readBinaryString(stream);
    this.format = readBinaryString(stream);

    final int argsLength = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    if (argsLength > 0) {
      this.args = new Object[argsLength];
      for (int i = 0; i < argsLength; ++i) {
        this.args[i] = readBinaryString(stream);
      }
    } else {
      this.args = null;
    }

    this.stackTrace = readBinaryString(stream);
    return true;
  }
}
