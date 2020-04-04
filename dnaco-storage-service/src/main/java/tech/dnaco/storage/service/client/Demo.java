package tech.dnaco.storage.service.client;

import io.netty.buffer.ByteBuf;
import tech.dnaco.server.ClientEventLoop;

public class Demo {
  public static void main(String[] args) throws Exception {
    try (ClientEventLoop eventLoop = new ClientEventLoop(1)) {
      final DnacoClientFactory factory = new DnacoClientFactory(eventLoop, "127.0.0.1", 25057);
      try (DnacoClient client = factory.newClient()) {
        final ByteBuf tenantId = DnacoClient.wrapBuffer("test-tenant");
        System.out.println("INC: " + client.incrementAndGet(tenantId, DnacoClient.wrapBuffer("counter-1")));
        System.out.println("INC: " + client.incrementAndGet(tenantId, DnacoClient.wrapBuffer("counter-1")));

        try (DnacoTransaction txn = client.newTransaction(tenantId)) {
          final DnacoKeyValue kv = new DnacoKeyValue(txn);
          //kv.insert(path, value);
          txn.commit();
        }
      }
    }
  }
}