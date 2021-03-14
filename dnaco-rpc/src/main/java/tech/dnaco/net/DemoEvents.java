package tech.dnaco.net;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import tech.dnaco.net.rpc.DnacoRpcClient;
import tech.dnaco.net.rpc.DnacoRpcDispatcher;
import tech.dnaco.net.rpc.DnacoRpcEvent;
import tech.dnaco.net.rpc.DnacoRpcHandler;
import tech.dnaco.net.rpc.DnacoRpcObjectMapper;
import tech.dnaco.net.rpc.DnacoRpcService;
import tech.dnaco.net.rpc.DnacoRpcSession;
import tech.dnaco.threading.ShutdownUtil;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.tracing.Tracer;

public class DemoEvents {
  public static void main(final String[] args) throws Exception {
    try (ServiceEventLoop eventLoop = new ServiceEventLoop(1, 1)) {
      final DnacoRpcDispatcher dispatcher = new DnacoRpcDispatcher(DnacoRpcObjectMapper.RPC_CBOR_OBJECT_MAPPER);
      final TestHandler testHandler = new TestHandler();
      dispatcher.addHandler(testHandler);

      final DnacoRpcService rawService = new DnacoRpcService(dispatcher);
      rawService.bindTcpService(eventLoop, 57025);

      if (true) {
        //demoClient();
        eventLoop.getWorkerGroup().scheduleAtFixedRate(testHandler::eventEmitter, 2, 2, TimeUnit.SECONDS);
        demoListener();
      }

      ShutdownUtil.addShutdownHook("server", rawService);
      rawService.waitStopSignal();
    }
  }

  private static void demoClient() throws Exception {
    try (ClientEventLoop eloop = new ClientEventLoop(1)) {
      final DnacoRpcClient client = DnacoRpcClient.newTcpClient(eloop.getWorkerGroup(), eloop.getChannelClass(),
        RetryUtil.newFixedRetry(1000), DnacoRpcObjectMapper.RPC_CBOR_OBJECT_MAPPER);
      client.whenConnected((c) -> { System.out.println("CONNECTED"); c.setReady(); });
      client.connect("localhost", 57025);

      for (int i = 0; i < 10; ++i) {
        client.sendEvent("tesht-event", new DemoData(10, "hello"));
        ThreadUtil.sleep(1, TimeUnit.SECONDS);
      }

      //client.disconnect()
    }
  }

  private static void demoListener() throws Exception {
    try (ClientEventLoop eloop = new ClientEventLoop(1)) {
      final DnacoRpcClient client = DnacoRpcClient.newTcpClient(eloop.getWorkerGroup(), eloop.getChannelClass(),
        RetryUtil.newFixedRetry(1000), DnacoRpcObjectMapper.RPC_CBOR_OBJECT_MAPPER);
      client.whenConnected((c) -> { System.out.println("CONNECTED"); c.setReady(); });
      client.connect("localhost", 57025);

      client.subscribeToEvent("tesht-event", (x, event) -> System.out.println("CLIENT RECEIVED " + event));
      ThreadUtil.sleep(20, TimeUnit.SECONDS);

      //client.disconnect()
    }
  }

  public static final class TestHandler implements DnacoRpcHandler {
    private final Set<DnacoRpcSession> sessions = ConcurrentHashMap.newKeySet();

    @RpcSessionConnected
    public void onSessionConnected(final DnacoRpcSession session) {
      this.sessions.add(session);
    }

    @RpcSessionDisconnected
    public void onSessionDisconnected(final DnacoRpcSession session) {
      this.sessions.remove(session);
    }

    @RpcEvent("tesht-event")
    public void teshtEvent(final DemoData data) {
      System.out.println("SRV: RECEIVED EVENT " + data);

    }

    private int dataIndex = 0;
    public void eventEmitter() {
      dataIndex++;
      final DemoData data = new DemoData(dataIndex, "hello-" + dataIndex);
      try {
        final DnacoRpcEvent event = DnacoRpcEvent.alloc(Tracer.getCurrentTraceId(), Tracer.getCurrentSpanId(),
          1, 0,
          Unpooled.wrappedBuffer("tesht-event".getBytes(StandardCharsets.UTF_8)),
          DnacoRpcObjectMapper.RPC_CBOR_OBJECT_MAPPER.toBytes(data, data.getClass()));

        for (final DnacoRpcSession session: sessions) {
          session.writeAndFlush(event);
        }
      } catch (final IOException e) {
        // no-op
      }
    }
  }

  private static class DemoData {
    private int iVal;
    private String sVal;

    protected DemoData() {
      // no-op
    }

    protected DemoData(final int iVal, final String sVal) {
      this.iVal = iVal;
      this.sVal = sVal;
    }

    @Override
    public String toString() {
      return "DemoData [iVal=" + iVal + ", sVal=" + sVal + "]";
    }
  }
}
