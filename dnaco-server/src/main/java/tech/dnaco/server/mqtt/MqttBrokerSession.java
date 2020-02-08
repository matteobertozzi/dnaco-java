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

import io.netty.channel.ChannelHandlerContext;
import tech.dnaco.server.AbstractServiceSession;

public class MqttBrokerSession extends AbstractServiceSession {
  private final String clientId;
  private final String userName;

  protected MqttBrokerSession(final ChannelHandlerContext ctx, final String clientId, final String userName) {
    super(ctx);
    this.clientId = clientId;
    this.userName = userName;
  }

  public String getClientId() {
    return clientId;
  }

  public String getUserName() {
    return userName;
  }

  protected void write(final Object msg) {
    super.write(msg);
  }

  @Override
  public String toString() {
    return "MqttBrokerSession [clientId=" + clientId + ", userName=" + userName + "]";
  }
}
