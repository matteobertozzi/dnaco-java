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

package tech.dnaco.server.binary;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class BinaryPacket extends AbstractReferenceCounted {
  static final int MAGIC = 0x00_00_00_00;

  private static final ArrayBlockingQueue<BinaryPacket> allocQueue = new ArrayBlockingQueue<>(8);

  private long pkgId;
  private int command;
  private ByteBuf data;

  private BinaryPacket() {
    // no-op
  }

  public static BinaryPacket alloc(final long pkgId, final int command) {
    return alloc(pkgId, command, Unpooled.EMPTY_BUFFER);
  }

  public static BinaryPacket alloc(final long pkgId, final int command, final ByteBuf data) {
    BinaryPacket packet = allocQueue.poll();
    if (packet == null) packet = new BinaryPacket();
    return packet.set(pkgId, command, data);
  }

  @Override
  protected void deallocate() {
    if (data != Unpooled.EMPTY_BUFFER) {
      data.release();
    }
    allocQueue.offer(this);
  }

  private BinaryPacket set(final long pkgId, final int command, final ByteBuf data) {
    if (refCnt() == 0) this.setRefCnt(1);
    this.pkgId = pkgId;
    this.command = command;
    this.data = data;
    return this;
  }

  public static boolean isBinaryPacket(final int magic1, final int magic2) {
    return magic1 == ((BinaryPacket.MAGIC >> 24) & 0xff) && magic2 == ((BinaryPacket.MAGIC >> 16) & 0xff);
  }

  public long getPkgId() {
    return pkgId;
  }

  public int getCommand() {
    return command;
  }
/*
  public byte[] getDataBytes() {
    if (data == null) return BytesUtil.EMPTY_BYTES;

    final int length = data.readableBytes();
    if (length == 0) return BytesUtil.EMPTY_BYTES;

    final byte[] content = new byte[length];
    data.readBytes(content);
    data.resetReaderIndex();
    return content;
  }
*/
  public int getDataLength() {
    return data == null ? 0 : data.readableBytes();
  }

  public ByteBuf getData() {
    return data;
  }

  @Override
  public String toString() {
    return "BinaryPacket [command=" + Integer.toHexString(command) + ", pkgId=" + pkgId + "]";
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return this;
  }

  // ====================================================================================================
  //  Encode/Decode helpers
  // ====================================================================================================
  public static ByteBuf readByteString(final ByteBuf buf) {
    final int length = buf.readShort();
    return buf.readSlice(length);
  }

  public static ByteBuf readBlob(final ByteBuf buf) {
    final int length = buf.readInt();
    return buf.readSlice(length);
  }

  public static int writeByteString(final ByteBuf buf, final String value) {
    return writeByteString(buf, value.getBytes(StandardCharsets.UTF_8));
  }

  public static int writeByteString(final ByteBuf buf, final byte[] value) {
    buf.writeShort(value.length);
    buf.writeBytes(value);
    return 2 + value.length;
  }

  public static int writeByteString(final ByteBuf buf, final byte[] value, final int off, final int len) {
    buf.writeShort(len);
    buf.writeBytes(value, off, len);
    return 2 + len;
  }

  public static int writeByteString(final ByteBuf buf, final ByteBuf value) {
    final int len = value.readableBytes();
    buf.writeShort(len);
    buf.writeBytes(value.slice());
    return 2 + len;
  }
}
