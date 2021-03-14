/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.net.rpc;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCounted;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractService.AbstractServiceSession;
import tech.dnaco.net.rpc.DnacoRpcHandler.Async;
import tech.dnaco.net.rpc.DnacoRpcHandler.RpcEvent;
import tech.dnaco.net.rpc.DnacoRpcHandler.RpcRequest;
import tech.dnaco.net.rpc.DnacoRpcHandler.RpcSessionConnected;
import tech.dnaco.net.rpc.DnacoRpcHandler.RpcSessionDisconnected;
import tech.dnaco.strings.HumansUtil;

public class DnacoRpcDispatcher {
  private final HashMap<ByteBuf, RpcHandler> rpcRequestMapping = new HashMap<>(256);
  private final HashMap<ByteBuf, List<RpcHandler>> rpcEventMapping = new HashMap<>(256);
  private final ArrayList<RpcSessionEventHandler> rpcSessionDisconnectedMappings = new ArrayList<>();
  private final ArrayList<RpcSessionEventHandler> rpcSessionConnectedMappings = new ArrayList<>();
  private final DnacoRpcSessionFactory sessionFactory;
  private final DnacoRpcObjectMapper objectMapper;

  public DnacoRpcDispatcher(final DnacoRpcObjectMapper objectMapper) {
    this(NO_RPC_SESSION_FACTORY, objectMapper);
  }

  public DnacoRpcDispatcher(final DnacoRpcSessionFactory sessionFactory, final DnacoRpcObjectMapper objectMapper) {
    this.sessionFactory = sessionFactory;
    this.objectMapper = objectMapper;
  }

  // ====================================================================================================
  //  Handlers Related
  // ====================================================================================================
  public void addHandler(final DnacoRpcHandler handler) {
    final Method[] methods = handler.getClass().getMethods();
    if (ArrayUtil.isEmpty(methods)) {
      return;
    }

    for (int i = 0; i < methods.length; ++i) {
      final Method method = methods[i];
      if (method.isAnnotationPresent(RpcRequest.class)) {
        addRpcRequest(handler, method);
      } else if (method.isAnnotationPresent(RpcEvent.class)) {
        addRpcEvent(handler, method);
      } else if (method.isAnnotationPresent(RpcSessionConnected.class)) {
        addRpcSessionConnected(handler, method);
      } else if (method.isAnnotationPresent(RpcSessionDisconnected.class)) {
        addRpcSessionDisconnected(handler, method);
      }
    }
  }

  private boolean addRpcRequest(final DnacoRpcHandler handler, final Method method) {
    final RpcRequest rpcRequest = method.getAnnotation(RpcRequest.class);
    if (rpcRequest == null) return false;

    final ByteBuf reqId = Unpooled.wrappedBuffer(rpcRequest.value().getBytes(StandardCharsets.UTF_8));
    rpcRequestMapping.put(reqId, new RpcHandler(handler, method, objectMapper));
    Logger.debug("add rpc request: {} {}", reqId.toString(StandardCharsets.UTF_8), method);
    return true;
  }

  private boolean addRpcEvent(final DnacoRpcHandler handler, final Method method) {
    final RpcEvent rpcEvent = method.getAnnotation(RpcEvent.class);
    if (rpcEvent == null) return false;

    final ByteBuf reqId = Unpooled.wrappedBuffer(rpcEvent.value().getBytes(StandardCharsets.UTF_8));
    rpcEventMapping.computeIfAbsent(reqId, (k) -> new ArrayList<>()).add(new RpcHandler(handler, method, objectMapper));
    return true;
  }

  private boolean addRpcSessionConnected(final DnacoRpcHandler handler, final Method method) {
    final RpcSessionConnected annotation = method.getAnnotation(RpcSessionConnected.class);
    if (annotation == null) return false;

    rpcSessionConnectedMappings.add(new RpcSessionEventHandler(handler, method));
    return true;
  }

  private boolean addRpcSessionDisconnected(final DnacoRpcHandler handler, final Method method) {
    final RpcSessionDisconnected annotation = method.getAnnotation(RpcSessionDisconnected.class);
    if (annotation == null) return false;

    rpcSessionDisconnectedMappings.add(new RpcSessionEventHandler(handler, method));
    return true;
  }

  // ====================================================================================================
  //  Session Related
  // ====================================================================================================
  public interface DnacoRpcSessionFactory {
    DnacoRpcSession createSession(final ChannelHandlerContext ctx);
    void destroySession(final DnacoRpcSession session);
  }

  protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
    final DnacoRpcSession session = sessionFactory.createSession(ctx);
    if (session == null) {
      throw new UnsupportedOperationException("expected a session from " + sessionFactory + ", got null");
    }

    runSessionEvent(rpcSessionConnectedMappings, session);
    return session;
  }

  protected void sessionDisconnected(final AbstractServiceSession session) {
    runSessionEvent(rpcSessionDisconnectedMappings, (DnacoRpcSession)session);

    sessionFactory.destroySession((DnacoRpcSession) session);
  }

  private void runSessionEvent(final ArrayList<RpcSessionEventHandler> eventHandlers, final DnacoRpcSession session) {
    if (eventHandlers.isEmpty()) return;

    for (final RpcSessionEventHandler handler: eventHandlers) {
      try {
        handler.invoke(session);
      } catch (final Throwable e) {
        Logger.error(e, "{} failed while handling session: {}", handler, session);
      }
    }
  }

  private static final DnacoRpcSessionFactory NO_RPC_SESSION_FACTORY = new DnacoRpcSessionFactory() {
    @Override
    public DnacoRpcSession createSession(final ChannelHandlerContext ctx) {
      return new DnacoRpcSession(ctx);
    }

    @Override
    public void destroySession(final DnacoRpcSession session) {
      // no-op
    }
  };

  // ====================================================================================================
  //  Packet Management Related
  // ====================================================================================================
  protected void handlePacket(final DnacoRpcSession session, final DnacoRpcPacket msg) {
    switch (msg.getPacketType()) {
      case REQUEST:
        handleRpcRequest(session, (DnacoRpcRequest) msg);
        break;
      case RESPONSE:
        handleRpcResponse(session, (DnacoRpcResponse) msg);
        break;
      case EVENT:
        handleRpcEvent(session, (DnacoRpcEvent) msg);
        break;
      case CONTROL:
      default:
        Logger.error("invalid message packet {}, closing the connection", msg.getPacketId());
        session.close();
        break;
    }
  }

  private void handleRpcRequest(final DnacoRpcSession ctx, final DnacoRpcRequest request) {
    final long startNs = System.nanoTime();
    try {
      final RpcHandler handler = rpcRequestMapping.get(request.getRequestId());
      System.out.println("HANDLER FOR " + request.getRequestId().toString(StandardCharsets.UTF_8) + " -> " + handler);
      if (handler == null) {
        // TODO: NOT_FOUND
        writeRpcResponse(ctx, request, DnacoRpcResponse.OperationStatus.FAILED, startNs, Unpooled.wrappedBuffer("NOT_FOUND".getBytes()));
        return;
      }

      final Object result = handler.invoke(ctx, request, objectMapper);
      if (result == null) {
        if (handler.isAsync()) {
          // the result should be handled by the method itself, otherwise the client will get a timeout
        } else {
          writeRpcResponse(ctx, request, DnacoRpcResponse.OperationStatus.SUCCEEDED, startNs, Unpooled.EMPTY_BUFFER);
        }
        return;
      }

      if (result instanceof DnacoRpcResponse) {
        final DnacoRpcResponse response = (DnacoRpcResponse)result;
        // TODO: adjust traceId, packetId, queueTime, execTime, ...
        ctx.write(response);
        return;
      }

      if (result instanceof ByteBuf) {
        writeRpcResponse(ctx, request, DnacoRpcResponse.OperationStatus.SUCCEEDED, startNs, (ByteBuf)result);
        return;
      }

      // TODO: handle internal error
      Logger.warn("unexpected return value from method {}, marking request as failed: {}", handler.method, result);
      if (result instanceof ReferenceCounted) ((ReferenceCounted)result).release();
      writeRpcResponse(ctx, request, DnacoRpcResponse.OperationStatus.FAILED, startNs, Unpooled.wrappedBuffer("INTERNAL_SERVER_ERROR".getBytes()));
    } catch (final Throwable e) {
      Logger.error(e, "failed to execute request: {}", request);
      writeRpcResponse(ctx, request, DnacoRpcResponse.OperationStatus.FAILED, startNs, Unpooled.wrappedBuffer(e.getMessage().getBytes()));
    }
  }

  private void writeRpcResponse(final DnacoRpcSession ctx, final DnacoRpcRequest request, final DnacoRpcResponse.OperationStatus status,
      final long startNs, final ByteBuf data) {
    final long execTime = System.nanoTime() - startNs;
    ctx.write(DnacoRpcResponse.alloc(request.getTraceId(), request.getSpanId(), request.getPacketId(), status, startNs - request.getStampNs(), execTime, data));
  }

  private void handleRpcResponse(final DnacoRpcSession ctx, final DnacoRpcResponse response) {
  }

  private void handleRpcEvent(final DnacoRpcSession ctx, final DnacoRpcEvent event) {
    final long startNs = System.nanoTime();
    final List<RpcHandler> eventHandlers = rpcEventMapping.get(event.getEventId());
    if (eventHandlers == null) {
      Logger.debug("ignoring, event handler not found: {}", event);
      return;
    }

    for (final RpcHandler handler : eventHandlers) {
      //Logger.debug("handle rpc event: {} {}", handler, event);
      //ctx.executor().submit(() -> {
        try {
          final Object result = handler.invoke(ctx, event, objectMapper);
          if (result != null) {
            Logger.warn("unexpected return value for event {} method {}, ignoring it: {}", event.getEventId().toString(StandardCharsets.UTF_8), handler.method, result);
            if (result instanceof ReferenceCounted) ((ReferenceCounted)result).release();
          }
        } catch (final Throwable e) {
          Logger.error(e, "{} failed while handling event {}", handler, event);
        } finally {
          final long execTime = System.nanoTime() - startNs;
          Logger.debug("event {} handled by {} in {}", event, handler, HumansUtil.humanTimeNanos(execTime));
        }
      //});
    }
  }

  private static final class RpcSessionEventHandler {
    private final DnacoRpcHandler handler;
    private final Method method;
    private final boolean hasSessionParam;

    private RpcSessionEventHandler(final DnacoRpcHandler handler, final Method method) {
      this.handler = handler;
      this.method = method;

      if (method.getReturnType() != void.class && method.getReturnType() != Void.class) {
        throw new UnsupportedOperationException("expected void return type: " + method);
      }

      final Parameter[] rawParams = method.getParameters();
      if (rawParams.length > 1) {
        throw new UnsupportedOperationException("expected 0 or 1 parameter: " + method);
      }

      hasSessionParam = (rawParams.length != 0);
      if (hasSessionParam && !DnacoRpcSession.class.isAssignableFrom(rawParams[0].getType())) {
        throw new UnsupportedOperationException("param must be a DnacoRpcSession. got " + rawParams[0].getType() + " " + method);
      }
    }

    public void invoke(final DnacoRpcSession session) throws Exception {
      if (hasSessionParam) {
        method.invoke(handler, session);
      } else {
        method.invoke(handler);
      }
    }
  }

  private static final class RpcHandler {
    private final ParamMapper[] paramMappers;
    private final ResultMapper resultMapper;
    private final DnacoRpcHandler handler;
    private final Method method;
    private final boolean async;

    private RpcHandler(final DnacoRpcHandler handler, final Method method, final DnacoRpcObjectMapper objectMapper) {
      this.handler = handler;
      this.method = method;
      this.async = method.isAnnotationPresent(Async.class);

      // prepare method params mappers
      final Parameter[] rawParams = method.getParameters();
      this.paramMappers = new ParamMapper[rawParams.length];
      for (int i = 0; i < rawParams.length; ++i) {
        final Parameter rawParam = rawParams[i];
        final Class<?> paramType = rawParam.getType();
        if (DnacoRpcPacket.class.isAssignableFrom(paramType)) {
          paramMappers[i] = RpcPacketParamMapper.INSTANCE;
        } else if (DnacoRpcSession.class.isAssignableFrom(paramType)) {
          paramMappers[i] = RpcSessionParamMapper.INSTANCE;
        } else {
          paramMappers[i] = new RpcPacketDataParamMapper(rawParam.getName(), paramType);
        }
      }

      // prepare result param mappers
      final Class<?> resultType = method.getReturnType();
      if (resultType == void.class || resultType == Void.class) {
        this.resultMapper = RpcNoResultMapper.INSTANCE;
      } else if (DnacoRpcPacket.class.isAssignableFrom(resultType)) {
        this.resultMapper = RpcPacketResultMapper.INSTANCE;
      } else {
        this.resultMapper = new RpcPacketDataResultMapper(resultType);
      }
    }

    public boolean isAsync() {
      return async;
    }

    public Object invoke(final DnacoRpcSession session, final DnacoRpcPacket packet, final DnacoRpcObjectMapper objectMapper) throws Exception {
      // convert params
      final Object[] params = new Object[paramMappers.length];
      for (int i = 0; i < params.length; ++i) {
        params[i] = paramMappers[i].get(session, packet, objectMapper);
      }

      // invoke method
      final Object result = method.invoke(handler, params);

      // convert result
      return resultMapper.get(session, packet, result, objectMapper);
    }

    @Override
    public String toString() {
      return "RpcHandler [async=" + async + ", handler=" + handler + ", method=" + method + "]";
    }

    private interface ParamMapper {
      Object get(DnacoRpcSession session, DnacoRpcPacket packet, DnacoRpcObjectMapper objectMapper);
    }

    private interface ResultMapper {
      Object get(DnacoRpcSession session, DnacoRpcPacket request, Object result, DnacoRpcObjectMapper objectMapper);
    }

    private static final class RpcPacketParamMapper implements ParamMapper {
      private static final RpcPacketParamMapper INSTANCE = new RpcPacketParamMapper();

      @Override
      public Object get(final DnacoRpcSession session, final DnacoRpcPacket packet, final DnacoRpcObjectMapper objectMapper) {
        return packet;
      }
    }

    private static final class RpcSessionParamMapper implements ParamMapper {
      private static final RpcSessionParamMapper INSTANCE = new RpcSessionParamMapper();

      @Override
      public Object get(final DnacoRpcSession session, final DnacoRpcPacket packet, final DnacoRpcObjectMapper objectMapper) {
        return session;
      }
    }

    private static final class RpcPacketDataParamMapper implements ParamMapper {
      private final Class<?> type;
      private final String name;

      public RpcPacketDataParamMapper(final String name, final Class<?> type) {
        this.name = name;
        this.type = type;
      }

      @Override
      public Object get(final DnacoRpcSession session, final DnacoRpcPacket packet, final DnacoRpcObjectMapper objectMapper) {
        try {
          return objectMapper.fromBytes(packet.getData(), type);
        } catch (final IOException e) {
          throw new UnsupportedOperationException("failed to convert param " + name + " to " + type, e);
        }
      }
    }

    private static final class RpcNoResultMapper implements ResultMapper {
      private static final RpcNoResultMapper INSTANCE = new RpcNoResultMapper();

      private RpcNoResultMapper() {
        // no-op
      }

      @Override
      public Object get(final DnacoRpcSession session, final DnacoRpcPacket request, final Object result, final DnacoRpcObjectMapper objectMapper) {
        // result should be null anyway
        return null;
      }
    }

    private static final class RpcPacketResultMapper implements ResultMapper {
      private static final RpcPacketResultMapper INSTANCE = new RpcPacketResultMapper();

      private RpcPacketResultMapper() {
        // no-op
      }

      @Override
      public Object get(final DnacoRpcSession session, final DnacoRpcPacket request, final Object result, final DnacoRpcObjectMapper objectMapper) {
        // result is a packet, we can check if traceId and other stuff are matching the reqest packet
        return result;
      }
    }

    private static final class RpcPacketDataResultMapper implements ResultMapper {
      private final Class<?> type;

      private RpcPacketDataResultMapper(final Class<?> type) {
        this.type = type;
      }

      @Override
      public Object get(final DnacoRpcSession session, final DnacoRpcPacket request, final Object result, final DnacoRpcObjectMapper objectMapper) {
        try {
          return objectMapper.toBytes(result, type);
        } catch (final IOException e) {
          throw new UnsupportedOperationException("failed to convert result from " + type, e);
        }
      }
    }
  }
}
