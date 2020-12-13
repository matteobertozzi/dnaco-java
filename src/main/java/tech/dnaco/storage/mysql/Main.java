package tech.dnaco.storage.mysql;

import com.gullivernet.server.netty.eloop.ServerEventLoop;

import io.netty.bootstrap.ServerBootstrap;

public class Main {
  public static void main(final String[] args) throws Exception {
    try (ServerEventLoop eloop = new ServerEventLoop(1, 2)) {
      final ServerBootstrap b = new ServerBootstrap();
      b.group(eloop.getBossGroup(), eloop.getWorkerGroup())
       .channel(eloop.getServerChannelClass())
       //.handler(new LoggingHandler(LogLevel.INFO))
       .childHandler(new MySqlServerInitializer(null));

      System.out.println("MYSQL");
      b.bind(3376).sync().channel().closeFuture().sync();
    }
  }
}
