package tech.dnaco.storage.service.client;

import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.storage.service.binary.BinaryCommands;

public class DnacoKeyValue {
  private final DnacoTransaction transaction;

  public DnacoKeyValue(final DnacoTransaction transaction) {
    this.transaction = transaction;
  }

  public <T> T get(String path, Class<T> classOfT) throws DnacoException {
    return null;
  }

  public Set<String> getKeys(String path) throws DnacoException {
    return null;
  }

  public void insert(String path, Object value) throws DnacoException {

  }

  public void update(String path, Object value) throws DnacoException {

  }

  public void upsert(String path, Object value) throws DnacoException {

  }

  public void delete(String path) throws DnacoException {

  }

  public void put(final byte[] key, final byte[] value) throws DnacoException {
    put(Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(value));
  }

  public void put(final ByteBuf key, final ByteBuf value) throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, transaction.getTenantId());
    req.writeLong(transaction.getTxnId());
    req.writeShort(1);
    req.writeShort(key.readableBytes());
    req.writeBytes(key);
    req.writeShort(value.readableBytes());
    req.writeBytes(value);

    transaction.getClient().sendSync(BinaryCommands.CMD_KEYVAL_UPSERT, req);
  }

  public void delete(final byte[] key) throws DnacoException {
    delete(Unpooled.wrappedBuffer(key));
  }

  public void delete(final ByteBuf key) throws DnacoException {
    final ByteBuf req = PooledByteBufAllocator.DEFAULT.buffer();
    BinaryPacket.writeByteString(req, transaction.getTenantId());
    req.writeLong(transaction.getTxnId());
    req.writeShort(1);
    req.writeShort(key.readableBytes());
    req.writeBytes(key);

    transaction.getClient().sendSync(BinaryCommands.CMD_KEYVAL_DELETE, req);
  }
}