package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class ComKill extends MySqlPacket {
  public ComKill(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
    // TODO Auto-generated method stub
  }

  public static ComKill read(final ByteBuf in, final int sequenceId) {
    final ComKill com = new ComKill(sequenceId);
    in.skipBytes(in.readableBytes()); // TODO
    return com;
  }
}
