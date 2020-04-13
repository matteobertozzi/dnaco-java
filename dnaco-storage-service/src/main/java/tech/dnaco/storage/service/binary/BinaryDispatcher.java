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

public class BinaryDispatcher implements BinaryServiceListener {
	private final BinaryServiceListener[] handlers = new BinaryServiceListener[BinaryCommands.CMD_TYPE_MAX];

	public BinaryDispatcher(final ServiceEventLoop eventLoop) {
		handlers[BinaryCommands.CMD_TYPE_SYSTEM]  = new SystemHandler(eventLoop);
		handlers[BinaryCommands.CMD_TYPE_COUNTER] = new CounterHandler(eventLoop);
		handlers[BinaryCommands.CMD_TYPE_TXN]     = new TransactionHandler(eventLoop);
		handlers[BinaryCommands.CMD_TYPE_PUBSUB]  = new PubSubHandler(eventLoop);
		handlers[BinaryCommands.CMD_TYPE_KEYVAL]  = new KeyValueHandler(eventLoop);
	}

	@Override
	public void connect(final BinaryServiceSession session) {
		Logger.debug("connect {}", session);
		for (int i = 0; i < handlers.length; ++i) {
			handlers[i].connect(session);
		}
	}

	@Override
	public void disconnect(final BinaryServiceSession session) {
		Logger.debug("disconnect {}", session);
		for (int i = 0; i < handlers.length; ++i) {
			handlers[i].disconnect(session);
		}
	}

	@Override
	public void packetReceived(final BinaryServiceSession session, final BinaryPacket packet) {
    if (BinaryCommands.isWriteRequest(packet)) {
      // TODO
    }

		//Logger.debug("packet received {} {}", session, packet);
		final BinaryServiceListener handler = handlers[BinaryCommands.getCmdType(packet)];
		if (handler != null) {
			handler.packetReceived(session, packet);
			return;
		}

		// unhandled
		Logger.error("invalid packet: {}", packet);
		session.write(BinaryCommands.newInvalidCommand(packet));
	}
}
