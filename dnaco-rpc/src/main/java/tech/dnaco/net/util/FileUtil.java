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

package tech.dnaco.net.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;

public final class FileUtil {
  private static final int BLOCK_SIZE = 4 << 10;
  private static final byte[] ZERO_BLOCK = new byte[BLOCK_SIZE];

  private FileUtil() {
    // no-op
  }

  public static FileChannel createZeroedFile(final Path path, final long length) throws IOException {
    return createFile(path, length, (byte) 0);
  }

  public static FileChannel createFile(final Path path, final long length, final byte value) throws IOException {
    final FileChannel channel = createEmptyFile(path);
    try {
      fill(channel, 0, length, value);
      return channel;
    } catch (final IOException e) {
      channel.close();
      throw e;
    }
  }

  public static FileChannel createEmptyFile(final Path path) throws IOException {
    return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
  }

  public static FileChannel openFile(final Path path) throws IOException {
    return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
  }

  public static void write(final Path path, final long offset, final ByteBuf data) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      write(channel, offset, data);
    }
  }

  public static void write(final FileChannel channel, final long offset, final ByteBuf data) throws IOException {
    channel.position(offset);
    if (data.nioBufferCount() > 1) {
      final ByteBuffer[] buffers = data.nioBuffers();
      for (int i = 0; i < buffers.length; ++i) {
        writeFully(channel, buffers[i]);
      }
    } else {
      writeFully(channel, data.nioBuffer());
    }
  }

  public static void write(final FileChannel channel, final long offset, final ByteBuffer buffer) throws IOException {
    channel.position(offset);
    writeFully(channel, buffer);
  }

  public static void fill(final FileChannel channel, final long offset, final long length, final byte value) throws IOException {
    final byte[] filler;
    if (value == 0) {
      filler = ZERO_BLOCK;
    } else {
      filler = new byte[BLOCK_SIZE];
      Arrays.fill(filler, value);
    }

    final ByteBuffer byteBuffer = ByteBuffer.wrap(filler);
    channel.position(offset);

    final int blocks = (int)(length / BLOCK_SIZE);
    final int blockRemainder = (int)(length % BLOCK_SIZE);
    for (int i = 0; i < blocks; ++i) {
      byteBuffer.position(0);
      writeFully(channel, byteBuffer);
    }

    if (blockRemainder > 0) {
      byteBuffer.position(0);
      byteBuffer.limit(blockRemainder);
      writeFully(channel, byteBuffer);
    }
  }

  private static void writeFully(final FileChannel channel, final ByteBuffer buffer) throws IOException {
    do {
      channel.write(buffer);
    } while (buffer.remaining() > 0);
  }
}
