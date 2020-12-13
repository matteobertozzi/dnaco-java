package tech.dnaco.storage.mysql;

import java.util.List;

import tech.dnaco.storage.mysql.protocol.AuthResponse;
import tech.dnaco.storage.mysql.protocol.BytesUtil;
import tech.dnaco.storage.mysql.protocol.ComKill;
import tech.dnaco.storage.mysql.protocol.ComPing;
import tech.dnaco.storage.mysql.protocol.ComQuery;
import tech.dnaco.storage.mysql.protocol.ComQuit;
import tech.dnaco.storage.mysql.protocol.Flags;
import tech.dnaco.storage.mysql.protocol.HandshakeResponse;
import tech.dnaco.storage.mysql.protocol.OkResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class MySqlPacketDecoder extends ByteToMessageDecoder {
  private static final int HEADER_SIZE = 4;

  enum State {
    HandshakeResponse,
    HandshakeAuth,
    Command,
  }

  private State state = State.HandshakeResponse;

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
    //System.out.println("MySql Decoder " + in.readableBytes());
    //System.out.println(ByteBufUtil.prettyHexDump(in));
    in.resetReaderIndex();

    if (in.readableBytes() < HEADER_SIZE) return;

    //System.out.println("READABLE BYTES offset=" + in.readerIndex() + " -> " + in.readableBytes());

    final int length = BytesUtil.getFixedInt(in, 3);
    if (in.readableBytes() < length) {
      System.out.println("packet length NOT available " + length + "/" + in.readableBytes());
      in.resetReaderIndex();
      ctx.close();
      return;
    }

    final int sequenceId = in.readByte();
    switch (state) {
      case HandshakeResponse:
        ctx.fireChannelRead(HandshakeResponse.read(in, sequenceId));
        state = State.HandshakeAuth;  // should this be demanded to the executor?
        break;
      case HandshakeAuth:
        ctx.fireChannelRead(AuthResponse.read(in, sequenceId));
        state = State.Command; // should this be demanded to the executor?
        break;
      case Command:
        final byte v = in.readByte();
        switch (v) {
          case Flags.COM_QUERY:
            System.out.println("---> COM QUERY");
            ctx.fireChannelRead(ComQuery.read(in, sequenceId));
            break;
          case Flags.COM_PROCESS_KILL:
            System.out.println("---> COM KILL");
            ctx.fireChannelRead(ComKill.read(in, sequenceId));
            break;
          case Flags.COM_PING:
            System.out.println("---> COM PING");
            ctx.fireChannelRead(ComPing.read(in, sequenceId));
            break;
          case Flags.COM_QUIT:
            System.out.println("---> COM QUIT");
            ctx.fireChannelRead(ComQuit.read(in, sequenceId));
            break;
          default:
            // TODO
            System.out.println("COMMAND " + v);
            ctx.writeAndFlush(new OkResponse(sequenceId));
            break;
        }
        break;
      default:
        // TODO: unhandled state?
        final byte[] data = new byte[length];
        in.readBytes(data);
        break;
    }

    in.skipBytes(in.readableBytes());
  }
}
