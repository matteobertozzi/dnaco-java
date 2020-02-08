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

package tech.dnaco.server.mqtt;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.server.AbstractService;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.strings.HumansUtil;

public final class MqttBrokerService extends AbstractService {
  private static final AttributeKey<MqttBrokerSession> ATTR_KEY_SESSION = AttributeKey.valueOf("sid");

  private final MqttBroker mqttBroker = new MqttBroker();

	protected MqttBrokerService(final ServiceEventLoop eventLoop) {
		setBootstrap(newTcpServerBootstrap(eventLoop, new ChannelInitializer<SocketChannel>() {
      private final MqttBrokerServiceHandler handler = new MqttBrokerServiceHandler(mqttBroker);

			@Override
			protected void initChannel(final SocketChannel channel) throws Exception {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new MqttDecoder());
        pipeline.addLast(MqttEncoder.INSTANCE);
        pipeline.addLast(handler);
			}
    }));
  }

  @Sharable
  private static class MqttBrokerServiceHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private final MqttBroker mqttBroker;

    private MqttBrokerServiceHandler(final MqttBroker mqttBroker) {
      this.mqttBroker = mqttBroker;
    }

    // https://www.hivemq.com/blog/mqtt-essentials-part-3-client-broker-connection-establishment/
    private void handleMqttConnect(final ChannelHandlerContext ctx, final MqttConnectMessage msg) {
      final MqttConnectVariableHeader header = msg.variableHeader();
      final MqttConnectPayload payload = msg.payload();

      final MqttConnectReturnCode retCode;
      final boolean sessionPresent;
      if (!header.hasUserName()) {
        sessionPresent = false;
        retCode = MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
      } else if (!mqttBroker.authenticate(payload.clientIdentifier(), payload.userName(), payload.passwordInBytes())) {
        sessionPresent = false;
        retCode = MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
      } else {
        if (header.isWillFlag()) {
          mqttBroker.setWill(payload.clientIdentifier(), payload.userName(), payload.willTopic(),
            header.isWillRetain(), header.willQos(), payload.willMessageInBytes());
        }

        if (header.isCleanSession()) {
          sessionPresent = false;
          mqttBroker.removePersistentSession(payload.clientIdentifier(), payload.userName());
        } else {
          sessionPresent = mqttBroker.hasPersistentSession(payload.clientIdentifier(), payload.userName());
          mqttBroker.setPersistentSession(payload.clientIdentifier(), payload.userName());
        }

        if (header.keepAliveTimeSeconds() > 0) {
          ctx.channel().pipeline().addFirst(
            new IdleStateHandler((int)(1.5f * header.keepAliveTimeSeconds()), 0, 0, TimeUnit.SECONDS));
        }

        // create the new session
        final MqttBrokerSession session = new MqttBrokerSession(ctx, payload.clientIdentifier(), payload.userName());
        ctx.channel().attr(ATTR_KEY_SESSION).set(session);
        mqttBroker.connected(session);

        retCode = MqttConnectReturnCode.CONNECTION_ACCEPTED;
      }

      ctx.write(new MqttConnAckMessage(
        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
        new MqttConnAckVariableHeader(retCode, sessionPresent)
      ));
    }

    private void handleMqttSubscribe(final MqttBrokerSession session, final MqttSubscribeMessage msg) {
      final List<MqttTopicSubscription> topics = msg.payload().topicSubscriptions();

      int grantedQosIndex = 0;
      final int[] grantedQos = new int[topics.size()];
      for (MqttTopicSubscription topic : topics) {
        if (mqttBroker.subscribe(session.getClientId(), topic.topicName(), topic.qualityOfService())) {
          grantedQos[grantedQosIndex++] = topic.qualityOfService().value();
        } else {
          grantedQos[grantedQosIndex++] = MqttQoS.FAILURE.value();
        }
      }

      // send SubAck
      session.write(new MqttSubAckMessage(
        new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
        MqttMessageIdVariableHeader.from(msg.variableHeader().messageId()),
        new MqttSubAckPayload(grantedQos)
      ));

      // send retained messages
    }

    private void handleMqttUnsubscribe(final MqttBrokerSession session, final MqttUnsubscribeMessage msg) {
      for (String topicName : msg.payload().topics()) {
        mqttBroker.unsubscribe(session.getClientId(), topicName);
      }

      // send UnSubAck
      session.write(new MqttUnsubAckMessage(
        new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
        MqttMessageIdVariableHeader.from(msg.variableHeader().messageId())
      ));
    }

    private void handleMqttPublish(final MqttBrokerSession session, final MqttPublishMessage msg) {
      final MqttPublishVariableHeader header = msg.variableHeader();
      final MqttFixedHeader fixedHeader = msg.fixedHeader();

      mqttBroker.publish(session.getClientId(), header.packetId(),
        fixedHeader.qosLevel(), fixedHeader.isRetain(), fixedHeader.isDup(),
        header.topicName(), msg.payload());

      // TODO: This message must be sent multiple times untill the client respond with a PUBACK/PUBREL
      switch (msg.fixedHeader().qosLevel()) {
        case AT_MOST_ONCE:
          break;
        case AT_LEAST_ONCE:
          session.write(new MqttPubAckMessage(
              new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 0),
              MqttMessageIdVariableHeader.from(header.packetId())
          ));
          break;
        case EXACTLY_ONCE:
          session.write(new MqttPubAckMessage(
              new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE, false, 0),
              MqttMessageIdVariableHeader.from(header.packetId())
          ));
          break;
        default:
          throw new UnsupportedOperationException("unexpected publish qos: " + msg.fixedHeader().qosLevel());
      }
    }

    private void handleMqttPubAck(final MqttBrokerSession session, final MqttPubAckMessage msg) {
      mqttBroker.pubAck(session.getClientId(), msg.variableHeader().messageId());
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final MqttMessage msg) {
      final MqttBrokerSession session = ctx.channel().attr(ATTR_KEY_SESSION).get();

      Logger.setSession(LoggerSession.newSystemGeneralSession());
      final long startTime = System.nanoTime();

      Logger.trace("{} request received: {}", session, msg);
      switch (msg.fixedHeader().messageType()) {
        case CONNECT:
          handleMqttConnect(ctx, (MqttConnectMessage) msg);
          break;
        case DISCONNECT:
          mqttBroker.disconnected(session, true);
          ctx.close();
          break;
        case PINGREQ:
          ctx.writeAndFlush(MqttMessage.PINGRESP);
          break;
        case SUBSCRIBE:
          handleMqttSubscribe(session, (MqttSubscribeMessage)msg);
          break;
        case UNSUBSCRIBE:
          handleMqttUnsubscribe(session, (MqttUnsubscribeMessage)msg);
          break;
        case PUBLISH:
          handleMqttPublish(session, (MqttPublishMessage)msg);
          break;
        case PUBACK:
          // The broker is the SENDER of the original PUBLISH package
          // QoS level 1 guarantees that a message is delivered at least one time to the receiver.
          // The sender stores the message until it gets a PUBACK packet from the receiver that
          // acknowledges receipt of the message.
          // It is possible for a message to be sent or delivered multiple times.
          // When we receive this message we stop trying to send PUBLISH message for the associated packetId.
          handleMqttPubAck(session, (MqttPubAckMessage)msg);
          break;
        case PUBREC:
          // The broker is the SENDER of the original PUBLISH package
          ctx.write(new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_MOST_ONCE, false, 0),
            msg.variableHeader()
          ));
          break;
        case PUBREL:
          // The broker is the RECEIVER of the original PUBLISH package
          // After the receiver gets the PUBREL packet, it can discard all stored states
          // and answer with a PUBCOMP packet (the same is true when the sender receives the PUBCOMP)
          ctx.write(new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
            msg.variableHeader()
          ));
          break;
        case PUBCOMP:
          // The broker is the SENDER of the original PUBLISH package
          // QoS level 2 guarantees that a message is delivered exactly one time to the receiver.
          // When we receive this message we clear all the save data for the associated packetId.
          break;

        // Message types not handled by the broker
        case CONNACK:
        case PINGRESP:
        case SUBACK:
        case UNSUBACK:
        default:
          throw new UnsupportedOperationException("unexpected mqtt packet " + msg.fixedHeader().messageType());
      }

      final long elapsed = System.nanoTime() - startTime;
      Logger.info("service request: {}", HumansUtil.humanTimeNanos(elapsed));
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
      Logger.debug("channel unregistered: {}", ctx.channel().remoteAddress());
      ctx.fireChannelUnregistered();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent && IdleState.READER_IDLE == ((IdleStateEvent) evt).state()) {
        final MqttBrokerSession session = ctx.channel().attr(ATTR_KEY_SESSION).get();
        mqttBroker.disconnected(session, false);
        ctx.close();
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      final MqttBrokerSession session = ctx.channel().attr(ATTR_KEY_SESSION).get();
      Logger.error(cause, "{} uncaught exception", session);
      mqttBroker.disconnected(session, false);
      ctx.close();
    }
  }

  public static void main(final String[] args) throws Exception {
    Logger.setSession(LoggerSession.newSession(LoggerSession.SYSTEM_PROJECT_ID, "service", null, LogLevel.TRACE, 0));

    try (ServiceEventLoop eloop = new ServiceEventLoop(1, Runtime.getRuntime().availableProcessors())) {
      final MqttBrokerService service = new MqttBrokerService(eloop);
      service.bind(57025);
      service.addShutdownHook();
      service.waitStopSignal();
    }
  }
}
