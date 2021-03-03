package tech.dnaco.storage.net;

import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import tech.dnaco.net.rpc.DnacoRpcHandler;
import tech.dnaco.net.rpc.DnacoRpcSession;

public class EventStorageRpcHandler implements DnacoRpcHandler {
  private final ConcurrentHashMap<ByteBuf, ChannelGroup> groups = new ConcurrentHashMap<>();

  @RpcRequest("/v0/event/emit")
  public void eventEmit(final DnacoRpcSession session, final Event event) {
  }

  @RpcRequest("/v0/event/subscribe")
  public void eventSubscribe(final DnacoRpcSession session, final EventSubscription subscription) {
    groups.computeIfAbsent(null, (k) -> new DefaultChannelGroup(session.executor()));
  }

  @RpcRequest("/v0/event/unsubscribe")
  public void eventUnsubscribe(final DnacoRpcSession session, final EventSubscription subscription) {
  }

  public static final class Event {

  }

  public static final class EventSubscription {

  }
}