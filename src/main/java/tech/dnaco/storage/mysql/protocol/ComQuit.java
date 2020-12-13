package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class ComQuit extends MySqlPacket {
  public ComQuit(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
    // TODO Auto-generated method stub
  }

  public static ComQuit read(final ByteBuf in, final int sequenceId) {
    final ComQuit com = new ComQuit(sequenceId);
    in.skipBytes(in.readableBytes()); // TODO
    return com;
  }
}
