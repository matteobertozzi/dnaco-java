package tech.dnaco.storage.service.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import tech.dnaco.server.binary.BinaryPacket;

public final class BinaryCommands {
  public static final int CMD_FLAG_MSG  = 0b111111_0000000000_0000000000000000; //  6bit
  public static final int CMD_FLAG_TYPE = 0b000000_1111111111_0000000000000000; // 10bit
  public static final int CMD_FLAG_CMD  = 0b000000_0000000000_1111111111111111; // 16bit

  public static final int CMD_TYPE_SYSTEM  = 0x000;
  public static final int CMD_TYPE_COUNTER = 0x001;
  public static final int CMD_TYPE_TXN     = 0x002;
  public static final int CMD_TYPE_PUBSUB  = 0x003;
  public static final int CMD_TYPE_KEYVAL  = 0x004;
  public static final int CMD_TYPE_MAX     = 0x005; // EOF 0x3ff

  private static final int CMD_MSG_REQ_READ   = 0b000000;
  private static final int CMD_MSG_REQ_WRITE  = 0b000001;
  private static final int CMD_MSG_RESP_OK    = 0b000010;
  private static final int CMD_MSG_RESP_ERROR = 0b000011;

  // system module commands
  public static final int CMD_SYSTEM_PING = (CMD_MSG_REQ_READ  << 26) | (CMD_TYPE_SYSTEM << 16) | 0x0001;
  public static final int CMD_SYSTEM_ECHO = (CMD_MSG_REQ_READ  << 26) | (CMD_TYPE_SYSTEM << 16) | 0x0002;

  // counter module commands
  public static final int CMD_COUNTER_INC = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_COUNTER << 16) | 0x0001;
  public static final int CMD_COUNTER_ADD = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_COUNTER << 16) | 0x0002;

  // transaction module commands
  public static final int CMD_TRANSACTION_CREATE   = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_TXN << 16) | 0x0001;
  public static final int CMD_TRANSACTION_OPEN     = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_TXN << 16) | 0x0002;
  public static final int CMD_TRANSACTION_COMMIT   = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_TXN << 16) | 0x0003;
  public static final int CMD_TRANSACTION_ROLLBACK = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_TXN << 16) | 0x0004;

  // pubsub module commands
  public static final int CMD_PUBSUB_SUBSCRIBE   = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_PUBSUB << 16) | 0x00001;
  public static final int CMD_PUBSUB_UNSUBSCRIBE = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_PUBSUB << 16) | 0x00002;
  public static final int CMD_PUBSUB_PUBLISH     = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_PUBSUB << 16) | 0x00003;

  // key/value module commands
  public static final int CMD_KEYVAL_INSERT = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_KEYVAL << 16) | 0x00001;
  public static final int CMD_KEYVAL_UPDATE = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_KEYVAL << 16) | 0x00002;
  public static final int CMD_KEYVAL_UPSERT = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_KEYVAL << 16) | 0x00003;
  public static final int CMD_KEYVAL_DELETE = (CMD_MSG_REQ_WRITE << 26) | (CMD_TYPE_KEYVAL << 16) | 0x00004;
  public static final int CMD_KEYVAL_GET    = (CMD_MSG_REQ_READ  << 26) | (CMD_TYPE_KEYVAL << 16) | 0x00005;
  public static final int CMD_KEYVAL_SCAN   = (CMD_MSG_REQ_READ  << 26) | (CMD_TYPE_KEYVAL << 16) | 0x00006;

  private BinaryCommands() {
    // no-op
  }

  // ================================================================================
  //  Command
  // ================================================================================
  public static int getCmd(final BinaryPacket packet) {
    return packet.getCommand() & 0x3ffffff;
  }

  public static int getCmdType(final BinaryPacket packet) {
    return (packet.getCommand() & CMD_FLAG_TYPE) >> 16;
  }

  private static int getCmdFlags(final BinaryPacket packet) {
    return (packet.getCommand() & CMD_FLAG_MSG) >> 26;
  }

  public static boolean isWriteRequest(final BinaryPacket packet) {
    return (getCmdFlags(packet) & CMD_MSG_REQ_WRITE) != 0;
  }

  public static boolean isOkResponse(final BinaryPacket packet) {
    return (getCmdFlags(packet) & CMD_MSG_RESP_OK) != 0;
  }

  public static boolean isErrorResponse(final BinaryPacket packet) {
    return (getCmdFlags(packet) & CMD_MSG_RESP_ERROR) != 0;
  }

  // ================================================================================
  //  Packet builder
  // ================================================================================
  public static BinaryPacket newRequest(final long pkgId, final int cmd) {
    return BinaryPacket.alloc(pkgId, cmd);
  }

  public static BinaryPacket newRequest(final long pkgId, final int cmd, final ByteBuf data) {
    return BinaryPacket.alloc(pkgId, cmd, data);
  }

  public static BinaryPacket newOkResponse(final BinaryPacket packet) {
    return newOkResponse(packet.getPkgId(), getCmd(packet), Unpooled.EMPTY_BUFFER);
  }

  public static BinaryPacket newOkResponse(final long pkgId, final int cmd) {
    return newOkResponse(pkgId, cmd, Unpooled.EMPTY_BUFFER);
  }

  public static BinaryPacket newOkResponse(final long pkgId, final int cmd, final ByteBuf data) {
    return BinaryPacket.alloc(pkgId, (CMD_MSG_RESP_OK << 26) | cmd, data);
  }

  public static BinaryPacket newErrorResponse(final BinaryPacket packet) {
    return newOkResponse(packet.getPkgId(), getCmd(packet), Unpooled.EMPTY_BUFFER);
  }

  public static BinaryPacket newErrorResponse(final long pkgId, final int cmd) {
    return newErrorResponse(pkgId, cmd, Unpooled.EMPTY_BUFFER);
  }

  public static BinaryPacket newErrorResponse(final long pkgId, final int cmd, final ByteBuf data) {
    return BinaryPacket.alloc(pkgId, (CMD_MSG_RESP_ERROR << 26) | cmd, data);
  }

  public static BinaryPacket newInvalidCommand(final BinaryPacket request) {
    return newErrorResponse(request.getPkgId(), 1);
  }
}