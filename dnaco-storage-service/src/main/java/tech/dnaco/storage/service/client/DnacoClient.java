package tech.dnaco.storage.service.client;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.binary.BinaryClient;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.storage.service.binary.BinaryCommands;

public class DnacoClient implements AutoCloseable {
  private final BinaryClient client;
  private final long waitTimeoutMs = TimeUnit.SECONDS.toMillis(5);

  public DnacoClient(final BinaryClient client) {
    this.client = client;
  }

  @Override
  public void close() throws InterruptedException {
    client.disconnect();
  }

  // ==========================================================================================
  //  System
  // ==========================================================================================
  public void ping() throws DnacoException {
    sendSync(BinaryCommands.CMD_SYSTEM_PING, Unpooled.EMPTY_BUFFER);
  }

  // ==========================================================================================
  //  Counters
  // ==========================================================================================
  public long incrementAndGet(final byte[] tenantId, final byte[] key) throws DnacoException {
    return incrementAndGet(Unpooled.wrappedBuffer(tenantId), Unpooled.wrappedBuffer(key));
  }

  public long incrementAndGet(final ByteBuf tenantId, final ByteBuf key) throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    BinaryPacket.writeByteString(req, key);

    return sendSync(BinaryCommands.CMD_COUNTER_INC, req, (packet) -> packet.getData().readLong());
  }

  public long addAndGet(final byte[] tenantId, final byte[] key, final long delta) throws DnacoException {
    return addAndGet(Unpooled.wrappedBuffer(tenantId), Unpooled.wrappedBuffer(key), delta);
  }

  public long addAndGet(final ByteBuf tenantId, final ByteBuf key, final long delta) throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    BinaryPacket.writeByteString(req, key);
    req.writeLong(delta);

    return sendSync(BinaryCommands.CMD_COUNTER_ADD, req, (packet) -> packet.getData().readLong());
  }

  // ==============================================================================================================
  //  New/Open Transaction
  // ==============================================================================================================
  public DnacoTransaction newTransaction(final String tenant) throws DnacoException {
    return newTransaction(Unpooled.wrappedBuffer(tenant.getBytes(StandardCharsets.UTF_8)));
  }

  public DnacoTransaction newTransaction(final ByteBuf tenantId) throws DnacoException {
    return DnacoTransaction.newTransaction(this, tenantId);
  }

  public DnacoTransaction openTransaction(final String tenant, final long txnId) throws DnacoException {
    return openTransaction(Unpooled.wrappedBuffer(tenant.getBytes(StandardCharsets.UTF_8)), txnId);
  }

  public DnacoTransaction openTransaction(final ByteBuf tenantId, final long txnId) throws DnacoException {
    return DnacoTransaction.openTransaction(this, tenantId, txnId);
  }

  // ==========================================================================================
  //  Pub/Sub
  // ==========================================================================================
  public void subscribe(final ByteBuf tenantId, final String... topic) throws DnacoException {
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

  public void unsubscribe(final ByteBuf tenantId, final String... topic) throws DnacoException {
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

  public void publish(final ByteBuf tenantId, final String topic, final ByteBuf data) throws DnacoException {
    publish(tenantId, 0, topic, data);
  }

  public void publish(final ByteBuf tenantId, final long txnId, final String topic, final ByteBuf data) throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    req.writeLong(txnId);
    BinaryPacket.writeByteString(req, tenantId);
    BinaryPacket.writeByteString(req, topic);
    req.writeBytes(data);

    sendSync(BinaryCommands.CMD_PUBSUB_PUBLISH, req);
  }

  public static ByteBuf wrapBuffer(final String value) {
    return Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
  }

  // ==============================================================================================================
  //  Send command helpers
  // ==============================================================================================================
  protected void sendSync(final int cmd, final ByteBuf buf) throws DnacoException {
    sendSync(cmd, buf, Function.identity());
  }

  protected <T> T sendSync(final int cmd, final ByteBuf buf, final Function<BinaryPacket, T> resultExtractor)
      throws DnacoException {
    try {
      return client.send(cmd, buf)
        .thenApply(resultExtractor)
        .orTimeout(waitTimeoutMs, TimeUnit.MILLISECONDS)
        /*.whenComplete((result, exception) -> {
          Logger.debug("completed with result={} exception={}", result, exception);
          latch.release();
        })*/
        .get();
    } catch (ExecutionException e) {
      throw new DnacoException(e.getCause());
    } catch (InterruptedException e) {
      throw new DnacoException(e.getCause());
    }
  }
}