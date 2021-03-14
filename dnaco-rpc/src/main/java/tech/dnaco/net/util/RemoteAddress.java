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

package tech.dnaco.net.util;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.channel.Channel;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.NetUtil;
import tech.dnaco.strings.StringUtil;

public final class RemoteAddress {
  private static final String X_REAL_IP = "x-real-ip";
  private static final String X_FORWARDERD_FOR = "x-forwarded-for";

  private RemoteAddress() {
    // no-op
  }

  public static String getRemoteAddress(final Channel channel) {
    return getRemoteAddress(channel, null);
  }

  public static String getRemoteAddress(final Channel channel, final HttpRequest request) {
    final SocketAddress remoteAddr = channel.remoteAddress();
    if (remoteAddr == null) {
      final String defaultAddr;
      if (channel instanceof DomainSocketChannel) {
        defaultAddr = "(unix)";
      } else {
        defaultAddr = "(unknown)";
      }
      return StringUtil.defaultIfEmpty(getRemoteAddressFromHeaders(request), defaultAddr);
    }

    if (!(remoteAddr instanceof InetSocketAddress)) return remoteAddr.toString();

    final InetSocketAddress inetAddr = (InetSocketAddress)remoteAddr;
    if (!inetAddr.getAddress().isLoopbackAddress()) {
      return NetUtil.toSocketAddressString(inetAddr);
    }

    final String addr = getRemoteAddressFromHeaders(request);
    if (addr != null) return addr;

    return NetUtil.toSocketAddressString(inetAddr);
  }

  private static String getRemoteAddressFromHeaders(final HttpRequest request) {
    if (request == null) return null;

    final HttpHeaders headers = request.headers();

    // if the inet address is localhost (e.g. requests are forwarded from nginx)
    // try to find the ip from the request headers
    final String realIp = headers.get(X_REAL_IP);
    if (StringUtil.isNotEmpty(realIp)) return realIp;

    final String forwarderIp = headers.get(X_FORWARDERD_FOR);
    if (StringUtil.isNotEmpty(forwarderIp)) return forwarderIp;

    return null;
  }
}
