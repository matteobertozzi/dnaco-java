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

package tech.dnaco.storage.service.binary;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.server.binary.BinaryService.BinaryServiceListener;
import tech.dnaco.server.binary.BinaryServiceSession;

public final class KeyValueHandler implements BinaryServiceListener {
  public KeyValueHandler(ServiceEventLoop eventLoop) {
    // no-op
	}

  @Override
	public void connect(final BinaryServiceSession session) {
		// no-op
	}

	@Override
	public void disconnect(final BinaryServiceSession session) {
		// no-op
	}

  @Override
  public void packetReceived(final BinaryServiceSession session, final BinaryPacket packet) {
    switch (packet.getCommand()) {
      case BinaryCommands.CMD_KEYVAL_INSERT:
        put(session, KeyValueOps.INSERT, packet);
        break;
      case BinaryCommands.CMD_KEYVAL_UPDATE:
        put(session, KeyValueOps.UPDATE, packet);
        break;
      case BinaryCommands.CMD_KEYVAL_UPSERT:
        put(session, KeyValueOps.UPSERT, packet);
        break;
      case BinaryCommands.CMD_KEYVAL_DELETE:
        delete(session, packet);
        break;
      case BinaryCommands.CMD_KEYVAL_GET:
        get(session, packet);
        break;
      case BinaryCommands.CMD_KEYVAL_SCAN:
        scan(session, packet);
        break;

      // unhandled
      default:
        Logger.error("invalid packet: {}", packet);
        session.write(BinaryCommands.newInvalidCommand(packet));
        break;
    }
  }

  public enum KeyValueOps { INSERT, UPDATE, UPSERT }

  private void put(final BinaryServiceSession session, final KeyValueOps ops, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    final int nItems = req.readShort();
    for (int i = 0; i < nItems; ++i) {
      final ByteBuf key = BinaryPacket.readByteString(req);
      final ByteBuf value = BinaryPacket.readBlob(req);
      Logger.info("{} {} txnId {} -> {} {}", tenantId.toString(StandardCharsets.UTF_8), ops, txnId, key.toString(StandardCharsets.UTF_8), value.toString(StandardCharsets.UTF_8));
    }
  }

  private void delete(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    final int nItems = req.readShort();
    for (int i = 0; i < nItems; ++i) {
      final ByteBuf key = BinaryPacket.readByteString(req);
      Logger.info("delete {} txnId {} -> {}", tenantId.toString(StandardCharsets.UTF_8), txnId, key.toString(StandardCharsets.UTF_8));
    }
  }

  private void get(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    final int nItems = req.readShort();
    for (int i = 0; i < nItems; ++i) {
      final ByteBuf key = BinaryPacket.readByteString(req);
      Logger.info("get {} txnId {} -> {}", tenantId.toString(StandardCharsets.UTF_8), txnId, key.toString(StandardCharsets.UTF_8));
    }
  }

  private void scan(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    final ByteBuf startKey = BinaryPacket.readByteString(req);
    final ByteBuf endKey = BinaryPacket.readByteString(req);
    Logger.info("scan {} txnId {} -> {} - {}", tenantId.toString(StandardCharsets.UTF_8), txnId, startKey.toString(StandardCharsets.UTF_8), endKey.toString(StandardCharsets.UTF_8));
  }
}