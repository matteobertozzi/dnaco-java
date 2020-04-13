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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.server.binary.BinaryService.BinaryServiceListener;
import tech.dnaco.server.binary.BinaryServiceSession;

public final class PubSubHandler implements BinaryServiceListener {
  private static final ConcurrentHashMap<BinaryServiceSession, Long> subs = new ConcurrentHashMap<>();

  public PubSubHandler(ServiceEventLoop eventLoop) {
    eventLoop.scheduleAtFixedRate(5, TimeUnit.SECONDS, new PubSubSender());
  }

  private static final class PubSubSender implements Runnable {
    private long index = 0;
    @Override
    public void run() {
      Logger.debug("sending pub/sub messages: {}", subs.size());
      for (Map.Entry<BinaryServiceSession, Long> entry: subs.entrySet()) {
        final ByteBuf respBuf = PooledByteBufAllocator.DEFAULT.buffer(8);
        respBuf.writeLong(index++);
        Logger.debug("sending async message to session topic:{}", entry.getValue());
        entry.getKey().write(BinaryPacket.alloc(entry.getValue().longValue(), 0, respBuf));
      }
    }
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
      case BinaryCommands.CMD_PUBSUB_SUBSCRIBE:
        subscribe(session, packet);
        break;
      case BinaryCommands.CMD_PUBSUB_UNSUBSCRIBE:
        unsubscribe(session, packet);
        break;
      case BinaryCommands.CMD_PUBSUB_PUBLISH:
        publish(session, packet);
        break;

      // unhandled
      default:
        Logger.error("invalid packet: {}", packet);
        session.write(BinaryCommands.newInvalidCommand(packet));
        break;
    }
  }

  private void subscribe(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final int topicCount = req.readShort();
    for (int i = 0; i < topicCount; ++i) {
      final ByteBuf topic = BinaryPacket.readByteString(req);
      Logger.info(" -> SUBSCRIBE: {} {}", tenantId.toString(StandardCharsets.UTF_8), topic.toString(StandardCharsets.UTF_8));
    }

    subs.put(session, packet.getPkgId());
    session.write(BinaryCommands.newOkResponse(packet));
  }

  private void unsubscribe(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final int topicCount = req.readShort();
    for (int i = 0; i < topicCount; ++i) {
      final ByteBuf topic = BinaryPacket.readByteString(req);
      Logger.info(" -> UNSUBSCRIBE: {} {}", tenantId.toString(StandardCharsets.UTF_8), topic.toString(StandardCharsets.UTF_8));
    }

    subs.remove(session, packet.getPkgId());
    session.write(BinaryCommands.newOkResponse(packet));
  }

  private void publish(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    final ByteBuf topic = BinaryPacket.readByteString(req);
    final ByteBuf msg = BinaryPacket.readBlob(req);
    Logger.info(" -> PUBLISH: {} {} {} {}", tenantId.toString(StandardCharsets.UTF_8), txnId, topic.toString(StandardCharsets.UTF_8), msg.toString(StandardCharsets.UTF_8));

    subs.remove(session, packet.getPkgId());
    session.write(BinaryCommands.newOkResponse(packet));
  }
}