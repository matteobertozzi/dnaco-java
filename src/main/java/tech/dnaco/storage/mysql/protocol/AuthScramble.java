package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class AuthScramble extends MySqlPacket {
  private static final int SCRAMBLE_LENGTH = 20;

  public AuthScramble(final int sequenceId) {
    super(sequenceId);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void write(final ByteBuf out) {
    BytesUtil.writeFixedStr(out, SCRAMBLE_LENGTH + 1, "0123456789abcdef");
  }

}
