package tech.dnaco.storage.mysql;

import tech.dnaco.storage.mysql.protocol.Handshake;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

public class MySqlServerInitializer extends ChannelInitializer<SocketChannel> {
  private final SslContext sslCtx;

  public MySqlServerInitializer(final SslContext sslCtx) {
      this.sslCtx = sslCtx;
  }

  @Override
  public void initChannel(final SocketChannel ch) throws Exception {
      final ChannelPipeline pipeline = ch.pipeline();

      if (sslCtx != null) {
          pipeline.addLast(sslCtx.newHandler(ch.alloc()));
      }

      System.out.println("INIT CONNECTION");

      pipeline.addLast(new MySqlPacketEncoder());
      pipeline.addLast(new MySqlPacketDecoder());
      pipeline.addLast(new MySqlExecutor());

      // TODO: Move to channelRegistered/channelActive
      ch.writeAndFlush(new Handshake(0));
  }
}
