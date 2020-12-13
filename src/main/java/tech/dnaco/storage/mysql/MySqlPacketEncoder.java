package tech.dnaco.storage.mysql;

import tech.dnaco.storage.mysql.protocol.BytesUtil;
import tech.dnaco.storage.mysql.protocol.MySqlPacket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MySqlPacketEncoder extends MessageToByteEncoder<MySqlPacket> {
  private static final byte[] EMPTY_HEAD = new byte[] { 0, 0, 0 };

  @Override
  protected void encode(final ChannelHandlerContext ctx, final MySqlPacket msg, final ByteBuf out) throws Exception {
    //System.out.println("MySql Encoder " + msg);
    writePacket(out, msg, msg.getSequenceId());

    //System.out.println(ByteBufUtil.prettyHexDump(out));
    out.resetReaderIndex();
  }

  public static void writePacket(final ByteBuf out, final MySqlPacket msg, final int sequenceId) {
    final int headIndex = out.writerIndex();
    out.writeBytes(EMPTY_HEAD);
    BytesUtil.writeFixedInt(out, 1, sequenceId);

    msg.write(out);
    final int tailIndex = out.writerIndex();

    out.writerIndex(headIndex);
    BytesUtil.writeFixedInt(out, 3, tailIndex - (headIndex + 4));
    //System.out.println("POS " + out.writerIndex());
    out.writerIndex(tailIndex);
    //System.out.println("WRITE " + tailIndex);
  }
}
