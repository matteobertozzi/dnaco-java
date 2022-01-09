/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.net.message;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import tech.dnaco.net.AbstractClient.ClientPromise;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.message.DnacoMessageService.DnacoMessageServiceProcessor;
import tech.dnaco.time.RetryUtil;

public final class DemoMessageService {
  private static class DemoProcessor implements DnacoMessageServiceProcessor {
    @Override
    public void sessionMessageReceived(final ChannelHandlerContext ctx, final DnacoMessage msg) throws Exception {
      if (false) {
        ctx.writeAndFlush(msg.retain());
      } else {
        System.out.println(msg.packetId() + " RECEIVED: " + msg.metadataMap() + " -> " + msg.data().toString(StandardCharsets.UTF_8));

        final DnacoMetadataMap metadata = new DnacoMetadataMap(16);
        metadata.put("foo", "10");
        final DnacoMessage rezp = new DnacoMessage(msg.packetId(), metadata, Unpooled.wrappedBuffer("hello".getBytes()));
        ctx.writeAndFlush(rezp);
      }
    }
  }

  public static void main(final String[] args) throws Exception {
    try (ServiceEventLoop eloop = new ServiceEventLoop(1, 2)) {
      final DnacoMessageService service = new DnacoMessageService(new DemoProcessor());
      service.bindTcpService(eloop, 8889);
      service.addShutdownHook();

      final DnacoMessageClient client = DnacoMessageClient.newTcpClient(eloop.getWorkerGroup(), eloop.getClientChannelClass(), RetryUtil.newFixedRetry(1000));
      client.connect("localhost", 8889);
      while (!client.isReady()) Thread.yield();

      for (int i = 0; i < 10; ++i) {
        final ClientPromise<DnacoMessage> resp = client.sendMessage(new DnacoMetadataMap(Map.of("a", "10")), Unpooled.wrappedBuffer(("hello" + i).getBytes()));
        resp.whenComplete((response, exception) -> System.out.println("CLIENT HOOK " + response.metadataMap() + " -> " + response.data().toString(StandardCharsets.UTF_8)));
      }

      service.waitStopSignal();
    }
  }
}
