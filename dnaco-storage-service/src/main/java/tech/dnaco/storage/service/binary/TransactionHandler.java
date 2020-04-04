package tech.dnaco.storage.service.binary;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.server.binary.BinaryService.BinaryServiceListener;
import tech.dnaco.server.binary.BinaryServiceSession;

public class TransactionHandler implements BinaryServiceListener {
  private final AtomicLong nextTxnId = new AtomicLong();

  public TransactionHandler(ServiceEventLoop eventLoop) {
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
      case BinaryCommands.CMD_TRANSACTION_CREATE:
        createTxn(session, packet);
        break;
      case BinaryCommands.CMD_TRANSACTION_OPEN:
        openTxn(session, packet);
        break;
      case BinaryCommands.CMD_TRANSACTION_COMMIT:
        commitTxn(session, packet);
        break;
      case BinaryCommands.CMD_TRANSACTION_ROLLBACK:
        rollbackTxn(session, packet);
        break;

      // unhandled
      default:
        Logger.error("invalid packet: {}", packet);
        session.write(BinaryCommands.newInvalidCommand(packet));
        break;
    }
  }

  private void createTxn(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);

    final long txnId = nextTxnId.incrementAndGet();
    System.out.println(" ---> tenantId: " + tenantId.toString(StandardCharsets.UTF_8) + " NEW TXN " + txnId);

    final ByteBuf respBuf = PooledByteBufAllocator.DEFAULT.buffer(8);
    respBuf.writeLong(txnId);
    session.write(BinaryPacket.alloc(packet.getPkgId(), packet.getCommand(), respBuf));
  }

  private void openTxn(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    System.out.println(" ---> tenantId: " + tenantId.toString(StandardCharsets.UTF_8) + " OPEN TXN " + txnId);

    session.write(BinaryCommands.newInvalidCommand(packet));
  }

  private void commitTxn(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    System.out.println(" ---> tenantId: " + tenantId.toString(StandardCharsets.UTF_8) + " COMMIT TXN " + txnId);

    session.write(BinaryCommands.newInvalidCommand(packet));
  }

  private void rollbackTxn(final BinaryServiceSession session, final BinaryPacket packet) {
    final ByteBuf req = packet.getData();
    final ByteBuf tenantId = BinaryPacket.readByteString(req);
    final long txnId = req.readLong();
    System.out.println(" ---> tenantId: " + tenantId.toString(StandardCharsets.UTF_8) + " ROLLBACK TXN " + txnId);

    session.write(BinaryCommands.newInvalidCommand(packet));
  }
}