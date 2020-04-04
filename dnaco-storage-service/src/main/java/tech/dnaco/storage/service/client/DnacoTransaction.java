package tech.dnaco.storage.service.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.storage.service.binary.BinaryCommands;

public class DnacoTransaction implements AutoCloseable {
  private final DnacoClient client;
  private final ByteBuf tenantId;
  private final long txnId;

  private boolean completed = false;

	private DnacoTransaction(final DnacoClient client, final ByteBuf tenantId, final long txnId) {
    this.client = client;
    this.tenantId = tenantId.retain();
    this.txnId = txnId;
  }

	@Override
	public void close() throws DnacoException {
    if (!completed) rollback();
		this.tenantId.release();
  }

  public long getTxnId() {
    return txnId;
  }

  public ByteBuf getTenantId() {
    return tenantId;
  }

  public DnacoClient getClient() {
    return client;
  }

  // ==========================================================================================
  //  New/Open Transaction
  // ==========================================================================================
  public static DnacoTransaction newTransaction(final DnacoClient client, final ByteBuf tenantId)
      throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);

    final long txnId = client.sendSync(BinaryCommands.CMD_TRANSACTION_CREATE, req, (packet) -> packet.getData().readLong());

    return new DnacoTransaction(client, tenantId, txnId);
  }

  public static DnacoTransaction openTransaction(final DnacoClient client, final ByteBuf tenantId, final long txnId)
      throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);

    client.sendSync(BinaryCommands.CMD_TRANSACTION_OPEN, req);

    return new DnacoTransaction(client, tenantId, txnId);
  }

  // ==========================================================================================
  //  Commit/Rollback Transaction
  // ==========================================================================================
  public void commit() throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(txnId);

    client.sendSync(BinaryCommands.CMD_TRANSACTION_COMMIT, req);
    this.completed = true;
  }

  public void rollback() throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, tenantId);
    req.writeLong(txnId);

    client.sendSync(BinaryCommands.CMD_TRANSACTION_ROLLBACK, req);
    this.completed = true;
  }
}