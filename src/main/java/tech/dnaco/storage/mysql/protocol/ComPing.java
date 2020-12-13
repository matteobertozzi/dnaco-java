package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class ComPing extends MySqlPacket {
  protected ComPing(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
    // TODO Auto-generated method stub
  }

  public static ComPing read(final ByteBuf in, final int sequenceId) {
    final ComPing ping = new ComPing(sequenceId);
    in.skipBytes(in.readableBytes()); // TODO
    return ping;
  }
}
