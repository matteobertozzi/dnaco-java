package tech.dnaco.storage.service.binary;

import tech.dnaco.server.binary.BinaryPacket;

public final class BinaryCommands {
  public static final int CMD_INVALID = 0x000_00001;
  public static final int CMD_ACK     = 0x000_00002;
  public static final int CMD_NACK    = 0x000_00003;

  public static final int CMD_TYPE_SYSTEM  = 0x000;
  public static final int CMD_TYPE_COUNTER = 0x001;
  public static final int CMD_TYPE_TXN     = 0x002;
  public static final int CMD_TYPE_PUBSUB  = 0x003;
  public static final int CMD_TYPE_KEYVAL  = 0x004;
  public static final int CMD_TYPE_MAX     = 0x005; // EOF

  // system module commands
  public static final int CMD_SYSTEM_PING = (CMD_TYPE_SYSTEM << 20) | 0x00001;
  public static final int CMD_SYSTEM_ECHO = (CMD_TYPE_SYSTEM << 20) | 0x00002;

  // counter module commands
  public static final int CMD_COUNTER_INC = (CMD_TYPE_COUNTER << 20) | 0x00001;
  public static final int CMD_COUNTER_ADD = (CMD_TYPE_COUNTER << 20) | 0x00002;

  // transaction module commands
  public static final int CMD_TRANSACTION_CREATE   = (CMD_TYPE_TXN << 20) | 0x00001;
  public static final int CMD_TRANSACTION_OPEN     = (CMD_TYPE_TXN << 20) | 0x00002;
  public static final int CMD_TRANSACTION_COMMIT   = (CMD_TYPE_TXN << 20) | 0x00003;
  public static final int CMD_TRANSACTION_ROLLBACK = (CMD_TYPE_TXN << 20) | 0x00004;

  // pubsub module commands
  public static final int CMD_PUBSUB_SUBSCRIBE   = (CMD_TYPE_PUBSUB << 20) | 0x00001;
  public static final int CMD_PUBSUB_UNSUBSCRIBE = (CMD_TYPE_PUBSUB << 20) | 0x00002;
  public static final int CMD_PUBSUB_PUBLISH     = (CMD_TYPE_PUBSUB << 20) | 0x00003;

  // key/value module commands
  public static final int CMD_KEYVAL_INSERT = (CMD_TYPE_KEYVAL << 20) | 0x00001;
  public static final int CMD_KEYVAL_UPDATE = (CMD_TYPE_KEYVAL << 20) | 0x00002;
  public static final int CMD_KEYVAL_UPSERT = (CMD_TYPE_KEYVAL << 20) | 0x00003;
  public static final int CMD_KEYVAL_DELETE = (CMD_TYPE_KEYVAL << 20) | 0x00004;
  public static final int CMD_KEYVAL_GET    = (CMD_TYPE_KEYVAL << 20) | 0x00005;
  public static final int CMD_KEYVAL_SCAN   = (CMD_TYPE_KEYVAL << 20) | 0x00006;

  private BinaryCommands() {
    // no-op
  }

  public static BinaryPacket newInvalidCommand(final BinaryPacket request) {
    return BinaryPacket.alloc(request.getPkgId(), CMD_INVALID);
  }
}