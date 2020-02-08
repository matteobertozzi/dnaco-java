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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.mqtt.MqttQoS;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.logging.Logger;

public class MqttBroker {

	public boolean authenticate(final String clientId, final String userName, final byte[] password) {
    // TODO
    Logger.debug("clientId={} userName={} password={}", clientId, userName, BytesUtil.toHexString(password));
		return false;
	}

	public boolean hasPersistentSession(final String clientId, final String userName) {
    // TODO
    Logger.debug("clientId={} userName={}", clientId, userName);
		return false;
	}

  public void setPersistentSession(String clientId, String userName) {
    // TODO
    Logger.debug("clientId={} userName={}", clientId, userName);
  }

	public void removePersistentSession(final String clientId, final String userName) {
    // TODO
    Logger.debug("clientId={} userName={}", clientId, userName);
  }

  public void setWill(final String clientId, final String userName,
      final String topic, final boolean retain, final int qos, final byte[] message) {
    // TODO
    Logger.debug("clientId={} userName={} topic={} retain={} qos={} message",
      clientId, userName, topic, retain, qos, BytesUtil.toHexString(message));
  }

  public boolean subscribe(final String clientId, final String topic, final MqttQoS qos) {
    // TODO
    Logger.debug("clientId={} topic={} qos={}", clientId, topic, qos);
    return false;
  }

  public void unsubscribe(final String clientId, final String topic) {
    // TODO
    Logger.debug("clientId={} topic={}", clientId, topic);
  }

  public void publish(final String clientId, final int messageId, final MqttQoS qos,
      final boolean retain, final boolean dup, final String topic, final ByteBuf message) {
    Logger.debug("clientId={} messageId={} qos={} retain={} dup={} topic={} message={}",
      clientId, messageId, qos, retain, dup, topic, ByteBufUtil.hexDump(message));
    // TODO
  }

  public void pubAck(final String clientId, final int messageId) {
    // TODO
    Logger.debug("clientId={} messageId={}", clientId, messageId);
  }

  public void connected(MqttBrokerSession session) {
    // TODO
    Logger.debug("connected {}", session);
  }

  public void disconnected(MqttBrokerSession session, boolean gracefully) {
    // If the disconnection is due to a DISCONNECT message the broker
    // MUST discard any Will Message associated with the current connection
    // without publishing it, as described in Section 3.1.2.5
    // TODO
    Logger.debug("disconnected {}", session, gracefully);
  }
}
