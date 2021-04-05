package tech.dnaco.storage.net;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.rpc.DnacoRpcEvent;
import tech.dnaco.net.rpc.DnacoRpcHandler;
import tech.dnaco.net.rpc.DnacoRpcSession;
import tech.dnaco.tracing.Tracer;

public class EventStorageRpcHandler implements DnacoRpcHandler {
  private static final ByteBuf ANY_EVENT_ID = Unpooled.wrappedBuffer(new byte[] { '*' });

  private final ConcurrentHashMap<ByteBuf, ChannelGroup> groups = new ConcurrentHashMap<>();
  private final AtomicLong pkgId = new AtomicLong(0);

  @RpcRequest("/v0/event/emit")
  public void eventEmit(final DnacoRpcSession session, final Event event) {
    // lookup groups matching the eventId and all the sub-matching-eventIds.
    //    - aaa.bbb.ccc (eventId)
    //    - aaa.bbb.
    //    - aaa.
    //    - *
    final DnacoRpcEvent rpcEvent = DnacoRpcEvent.alloc(Tracer.getCurrentTraceId(), Tracer.getCurrentSpanId(),
      pkgId.incrementAndGet(), 0, event.eventId, event.data);

    final ByteBuf eventId = event.eventId;
    int offset = eventId.readableBytes();
    while (offset > 0) {
      final int dotIndex = ByteBufUtil.indexOf(eventId, offset, 0, (byte)'.');
      if (dotIndex < 0) break;

      final ByteBuf subEventId = eventId.slice(0, dotIndex + 1);
      final ChannelGroup group = groups.get(subEventId);
      if (group != null) group.writeAndFlush(rpcEvent);

      offset = dotIndex - 1;
    }

    final ChannelGroup group = groups.get(ANY_EVENT_ID);
    if (group != null) group.writeAndFlush(rpcEvent);
  }

  @RpcRequest("/v0/event/subscribe")
  public void eventSubscribe(final DnacoRpcSession session, final EventSubscription subscription) {
    final ByteBuf[] eventIds = subscription.eventIds;
    for (int i = 0; i < eventIds.length; ++i) {
      final ChannelGroup group = groups.computeIfAbsent(eventIds[i], (k) -> new DefaultChannelGroup(session.executor()));
      Logger.trace("register {} to eventId {}", session, eventIds[i].toString(StandardCharsets.UTF_8));
      session.addToGroup(group);
    }
  }

  @RpcRequest("/v0/event/unsubscribe")
  public void eventUnsubscribe(final DnacoRpcSession session, final EventSubscription subscription) {
    final ByteBuf[] eventIds = subscription.eventIds;
    for (int i = 0; i < eventIds.length; ++i) {
      final ChannelGroup group = groups.get(eventIds[i]);
      if (group == null) continue;

      Logger.trace("unregister {} from eventId {}", session, eventIds[i].toString(StandardCharsets.UTF_8));
      session.removeFromGroup(group);
    }
  }

  public static final class EventSubscription {
    private ByteBuf[] eventIds;
  }

  public static final class Event {
    private ByteBuf eventId;
    private ByteBuf data;
  }
}