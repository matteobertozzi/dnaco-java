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

package tech.dnaco.storage.service.client2;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.binary.BinaryClient;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.storage.service.binary.BinaryCommands;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.ThreadUtil;

public class StorageClient {
  private final BinaryClient client;
  private final ByteBuf tenantId;

  private final long waitTimeoutMs = TimeUnit.SECONDS.toMillis(5);

  public StorageClient(final BinaryClient client, final String tenantId) {
    this(client, tenantId.getBytes(StandardCharsets.UTF_8));
  }

  public StorageClient(final BinaryClient client, final byte[] tenantId) {
    this.client = client;
    this.tenantId = Unpooled.wrappedBuffer(tenantId);
  }

  private void sendSync(final int cmd, final ByteBuf buf) throws InterruptedException, ExecutionException {
    sendSync(cmd, buf, Function.identity());
  }

  private <T> T sendSync(final int cmd, final ByteBuf buf, final Function<BinaryPacket, T> resultExtractor)
      throws InterruptedException, ExecutionException {
    return client.send(cmd, buf)
      .thenApply(resultExtractor)
      .orTimeout(waitTimeoutMs, TimeUnit.MILLISECONDS)
      /*.whenComplete((result, exception) -> {
        Logger.debug("completed with result={} exception={}", result, exception);
        latch.release();
      })*/
      .get();
  }

  // ==========================================================================================
  //  System
  // ==========================================================================================
  public void ping() throws InterruptedException, ExecutionException {
    sendSync(BinaryCommands.CMD_SYSTEM_PING, Unpooled.EMPTY_BUFFER);
  }

  // ==========================================================================================
  //  Counters
  // ==========================================================================================
  public long incrementAndGet(final byte[] key) throws InterruptedException, ExecutionException {
    return incrementAndGet(Unpooled.wrappedBuffer(key));
  }

  public long incrementAndGet(final ByteBuf key) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    BinaryPacket.writeByteString(req, key);

    return sendSync(BinaryCommands.CMD_COUNTER_INC, req, (packet) -> packet.getData().readLong());
  }

  public long addAndGet(final byte[] key, final long delta) throws InterruptedException, ExecutionException {
    return addAndGet(Unpooled.wrappedBuffer(key), delta);
  }

  public long addAndGet(final ByteBuf key, final long delta) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    BinaryPacket.writeByteString(req, key);
    req.writeLong(delta);

    return sendSync(BinaryCommands.CMD_COUNTER_ADD, req, (packet) -> packet.getData().readLong());
  }

  // ==========================================================================================
  //  Transactions
  // ==========================================================================================
  public long newTransaction() throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);

    return sendSync(BinaryCommands.CMD_TRANSACTION_CREATE, req, (packet) -> packet.getData().readLong());
  }

  public void openTransaction(final long txnId) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(txnId);

    sendSync(BinaryCommands.CMD_TRANSACTION_OPEN, req);
  }

  public void commitTransaction(final long txnId) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(txnId);

    sendSync(BinaryCommands.CMD_TRANSACTION_COMMIT, req);
  }

  public void rollbackTransaction(final long txnId) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(txnId);

    sendSync(BinaryCommands.CMD_TRANSACTION_ROLLBACK, req);
  }

  // ==========================================================================================
  //  Pub/Sub
  // ==========================================================================================
  public void subscribe(final String... topic) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeShort(topic.length);
    for (int i = 0; i < topic.length; ++i) {
      BinaryPacket.writeByteString(req, topic[i]);
    }

    final BinaryPacket packet = sendSync(BinaryCommands.CMD_PUBSUB_SUBSCRIBE, req, Function.identity());
    Logger.debug("subscribe ack: {}", packet);
    client.subscribe(packet.getPkgId(), (notification) -> {
      Logger.debug("notified: {}", notification);
    });
  }

  public void unsubscribe(final String... topic) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeShort(topic.length);
    for (int i = 0; i < topic.length; ++i) {
      BinaryPacket.writeByteString(req, topic[i]);
    }

    final BinaryPacket packet = sendSync(BinaryCommands.CMD_PUBSUB_UNSUBSCRIBE, req, Function.identity());
    Logger.debug("unsubscribe: {}", packet);
    client.unsubscribe(packet.getPkgId());
  }

  public void publish(final String topic, final ByteBuf data) throws InterruptedException, ExecutionException {
    publish(0, topic, data);
  }

  public void publish(final long txnId, final String topic, final ByteBuf data) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    req.writeLong(txnId);
    BinaryPacket.writeByteString(req, tenantId);
    BinaryPacket.writeByteString(req, topic);
    req.writeBytes(data);

    sendSync(BinaryCommands.CMD_PUBSUB_PUBLISH, req);
  }

  // ==========================================================================================
  //  Key/Value
  // ==========================================================================================
  public void put(final ByteBuf key, final ByteBuf value) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(0);
    req.writeShort(1);
    req.writeShort(key.readableBytes());
    req.writeBytes(key);
    req.writeShort(value.readableBytes());
    req.writeBytes(value);

    sendSync(BinaryCommands.CMD_KEYVAL_UPSERT, req);
  }

  public void delete(final ByteBuf key) throws InterruptedException, ExecutionException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(0);
    req.writeShort(1);
    req.writeShort(key.readableBytes());
    req.writeBytes(key);

    sendSync(BinaryCommands.CMD_KEYVAL_DELETE, req);
  }

  // ==========================================================================================
  //  Test
  // ==========================================================================================
  private static final int NOPS = 1000_000;
  private static void runClient(final BinaryClient client) throws InterruptedException, ExecutionException {
    final ByteBuf kay = Unpooled.wrappedBuffer("key-0".getBytes(StandardCharsets.UTF_8));

    final StorageClient storage = new StorageClient(client, "test-tenant");
    long value = 0;
    final long startTime = System.nanoTime();
    for (int i = 0; i < NOPS; ++i) {
      //storage.ping();
      kay.resetWriterIndex();
      //value = storage.addAndGet(kay, 3);
      value = storage.incrementAndGet(kay);
    }
    long elapsed = System.nanoTime() - startTime;
    System.out.println(value
      + " -> " + HumansUtil.humanTimeNanos(elapsed)
      + " -> " + HumansUtil.humanRate((double)NOPS / (elapsed / 1000000000.0)));
  }

  public static long getUsedMemory() {
    final Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  public static void main(final String[] args) throws Exception {
    try (ServiceEventLoop eventLoop = new ServiceEventLoop(true, 1, 4)) {
      Thread[] thread = new Thread[1];
      for (int i = 0; i < thread.length; ++i) {
        thread[i] = new Thread(() -> {
          try {
            final BinaryClient client = new BinaryClient(eventLoop);
            client.connect("127.0.0.1", 25057);

            if (true) {
              final StorageClient storage = new StorageClient(client, "test-tenant");

              storage.subscribe("foo", "bar");
              ThreadUtil.sleep(10_000);
              storage.unsubscribe("foo", "bar");

              System.out.println("INC: " + storage.incrementAndGet("FOO".getBytes(StandardCharsets.UTF_8)));
              System.out.println("INC: " + storage.incrementAndGet("FOO".getBytes(StandardCharsets.UTF_8)));
              System.out.println("ADD: " + storage.addAndGet("FOO".getBytes(StandardCharsets.UTF_8), 2));
            } else {
              runClient(client);
            }

            System.out.println("disconnect");
            client.disconnect();
          } catch (Throwable e) {
            Logger.error(e, "bang!");
          }
        }, "client-" + i);
      }

      long startTime = System.nanoTime();
      for (int i = 0; i < thread.length; ++i) thread[i].start();
      for (int i = 0; i < thread.length; ++i) ThreadUtil.shutdown(thread[i]);
      long elapsed = System.nanoTime() - startTime;
      System.out.println(" -> " + HumansUtil.humanRate((double)(thread.length * NOPS) / (elapsed / 1000000000.0)));

      System.in.read();
    }
  }
}