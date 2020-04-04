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

package tech.dnaco.storage.service.binary;

import tech.dnaco.logging.Logger;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.binary.BinaryPacket;
import tech.dnaco.server.binary.BinaryService.BinaryServiceListener;
import tech.dnaco.server.binary.BinaryServiceSession;

public final class SystemHandler implements BinaryServiceListener {
  public SystemHandler(ServiceEventLoop eventLoop) {
    // no-op
	}

  @Override
	public void connect(final BinaryServiceSession session) {
		// no-op
	}

	@Override
	public void disconnect(final BinaryServiceSession session) {
		// no-op
	}

  @Override
  public void packetReceived(final BinaryServiceSession session, final BinaryPacket packet) {
    switch (packet.getCommand()) {
      case BinaryCommands.CMD_SYSTEM_PING:
        session.write(BinaryPacket.alloc(packet.getPkgId(), BinaryCommands.CMD_SYSTEM_PING));
        break;
      case BinaryCommands.CMD_SYSTEM_ECHO:
        session.write(packet);
        break;

      // unhandled
      default:
        Logger.error("invalid packet: {}", packet);
        session.write(BinaryCommands.newInvalidCommand(packet));
        break;
    }
  }
}