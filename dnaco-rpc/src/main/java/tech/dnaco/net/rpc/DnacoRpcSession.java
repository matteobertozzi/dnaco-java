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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import tech.dnaco.net.AbstractService.AbstractServiceSession;

public class DnacoRpcSession extends AbstractServiceSession {
  private Object data;

  protected DnacoRpcSession(final ChannelHandlerContext ctx) {
    super(ctx);
  }

  public Object getSessionId() {
    return data;
  }

  public boolean hasData() {
    return data != null;
  }

  @SuppressWarnings("unchecked")
  public <T> T getData() {
    return (T) data;
  }

  public void setData(final Object data) {
    this.data = data;
  }

  public boolean hasDataOfType(final Class<?> classOfT) {
    return data != null && classOfT.isAssignableFrom(data.getClass());
  }

  public void addToGroup(final ChannelGroup group) {
    group.add(getChannel());
  }

  public void removeFromGroup(final ChannelGroup group) {
    group.remove(getChannel());
  }
}
