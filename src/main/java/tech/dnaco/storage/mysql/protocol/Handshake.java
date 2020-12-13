package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class Handshake extends MySqlPacket {
  public long protocolVersion = 0x0a;
  public String serverVersion = "5.6.10";
  public long connectionId = 1;
  public String challenge1 = "01234567";
  public long capabilityFlags = CLIENT_FLAGS; // Flags.CLIENT_RESERVED | Flags.CLIENT_PROTOCOL_41; // 0xffff7fff;//Flags.CLIENT_PROTOCOL_41;
  public long characterSet = 0;
  public long statusFlags = 0;
  public String challenge2 = "";
  public long authPluginDataLength = 0;
  public String authPluginName = "";

  public static final int CLIENT_FLAGS = (
            Flags.CLIENT_LONG_PASSWORD
          | Flags.CLIENT_FOUND_ROWS
          | Flags.CLIENT_LONG_FLAG
          | Flags.CLIENT_CONNECT_WITH_DB
          | Flags.CLIENT_NO_SCHEMA
          | Flags.CLIENT_IGNORE_SPACE
          | Flags.CLIENT_PROTOCOL_41
          | Flags.CLIENT_INTERACTIVE
          | Flags.CLIENT_IGNORE_SIGPIPE
          | Flags.CLIENT_TRANSACTIONS
          | Flags.CLIENT_RESERVED
          | Flags.CLIENT_SECURE_CONNECTION
          | Flags.CLIENT_MULTI_STATEMENTS
          | Flags.CLIENT_MULTI_RESULTS
          | Flags.CLIENT_PS_MULTI_RESULTS
          | Flags.CLIENT_REMEMBER_OPTIONS
          | Flags.CLIENT_PLUGIN_AUTH
          | Flags.CLIENT_CONNECT_ATTRS
          | Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
          | Flags.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
  );

  public Handshake(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
    BytesUtil.writeFixedInt(out, 1, this.protocolVersion);
    BytesUtil.writeNullStr(out, this.serverVersion);
    BytesUtil.writeFixedInt(out, 4, this.connectionId);
    BytesUtil.writeFixedStr(out, 8, this.challenge1);
    BytesUtil.writeFiller(out, 1);
    BytesUtil.writeFixedInt(out, 2, this.capabilityFlags & 0xffff);
    BytesUtil.writeFixedInt(out, 1, this.characterSet);
    BytesUtil.writeFixedInt(out, 2, this.statusFlags);
    BytesUtil.writeFixedInt(out, 2, (this.capabilityFlags >> 16) & 0xffff);

    final int scrambleLength = 14;
    out.writeByte(scrambleLength);
    BytesUtil.writeFixedStr(out, 10, "");
    BytesUtil.writeFixedStr(out, 14, this.challenge2);

    out.writeByte(0);
  }
}
