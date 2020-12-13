package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class AuthResponse extends MySqlPacket {
  public AuthResponse(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
  }

  public static AuthResponse read(final ByteBuf in, final int sequenceId) {
    final AuthResponse auth = new AuthResponse(sequenceId);
    in.skipBytes(in.readableBytes()); // TODO
    return auth;
  }
}
