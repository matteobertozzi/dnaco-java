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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.server.binary.BinaryService.BinaryServiceListener;
import tech.dnaco.server.binary.BinaryServiceSession;

public final class CounterHandler implements BinaryServiceListener {
  private static final ConcurrentHashMap<ByteBuf, AtomicLong> counters = new ConcurrentHashMap<>();

  public CounterHandler(ServiceEventLoop eventLoop) {
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
      case BinaryCommands.CMD_COUNTER_INC:
        inc(session, packet);
        break;
      case BinaryCommands.CMD_COUNTER_ADD:
        add(session, packet);
        break;

      // unhandled
      default:
        Logger.error("invalid packet: {}", packet);
        session.write(BinaryCommands.newInvalidCommand(packet));
        break;
    }
  }

  // | klen 2bytes | key ... |
  private void inc(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final ByteBuf key = BinaryPacket.readByteString(req);

    final AtomicLong counter = getOrCreate(key);
    final long v = counter.incrementAndGet();
    sendResult(session, packet, v);
  }

  // | klen 2bytes | key ... | delta 8bytes |
  private void add(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final ByteBuf key = BinaryPacket.readByteString(req);
    final long delta = req.readLong();

    final AtomicLong counter = getOrCreate(key);
    final long v = counter.addAndGet(delta);
    sendResult(session, packet, v);
  }

  private void sendResult(final BinaryServiceSession session, final BinaryPacket packet, final long v) {
    final ByteBuf respBuf = PooledByteBufAllocator.DEFAULT.buffer(8);
    respBuf.writeLong(v);
    session.write(BinaryPacket.alloc(packet.getPkgId(), packet.getCommand(), respBuf));
  }

  private AtomicLong getOrCreate(final ByteBuf key) {
    final AtomicLong counter = counters.get(key);
    if (counter != null) return counter;

    final AtomicLong newCounter = new AtomicLong(0);
    final AtomicLong oldCounter = counters.putIfAbsent(key, newCounter);
    if (oldCounter != null) return oldCounter;

    key.retain();
    return newCounter;
  }
}
