package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class HandshakeResponse extends MySqlPacket {
  public long capabilityFlags = Flags.CLIENT_PROTOCOL_41;
  public long maxPacketSize = 0;
  public long characterSet = 0;
  public String username = "";
  public long authResponseLen = 0;
  public String authResponse = "";
  public String schema = "";
  public String pluginName = "";
  public long clientAttributesLen = 0;
  public String clientAttributes = "";

  public HandshakeResponse(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
    // TODO Auto-generated method stub
  }

  public boolean hasCapabilityFlag(final long flag) {
    return ((this.capabilityFlags & flag) == flag);
  }

  public static HandshakeResponse read(final ByteBuf in, final int sequenceId) {
    final HandshakeResponse resp = new HandshakeResponse(sequenceId);

    resp.capabilityFlags = BytesUtil.getFixedLong(in, 2);
    in.readerIndex(in.readerIndex() - 2);

    System.out.println("Capabilities " + resp.capabilityFlags);
    if (resp.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
      System.out.println("PROTO 41");
      resp.capabilityFlags = BytesUtil.getFixedLong(in, 4);
      resp.maxPacketSize = BytesUtil.getFixedLong(in, 4);
      resp.characterSet = BytesUtil.getFixedLong(in, 1);
      BytesUtil.skipFiller(in, 23);
      resp.username = BytesUtil.getNullStr(in);

      if (resp.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
        resp.authResponseLen = BytesUtil.getLenEncInt(in);
        resp.authResponse = BytesUtil.getFixedStr(in, (int) resp.authResponseLen, true);
        System.out.println("AUTH RESPONSE[0] " + resp.authResponse);
      } else {
        if (resp.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
          resp.authResponseLen = BytesUtil.getFixedLong(in, 1);
          resp.authResponse = BytesUtil.getFixedStr(in, (int) resp.authResponseLen, true);
        } else {
          resp.authResponse = BytesUtil.getNullStr(in);
        }
        System.out.println("AUTH RESPONSE[1] " + resp.authResponse);
      }

      if (resp.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB)) {
        resp.schema = BytesUtil.getNullStr(in);
        System.out.println("DB SCHEMA " + resp.schema);
      }

      if (resp.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
        resp.pluginName = BytesUtil.getNullStr(in);
        System.out.println("AUTH PLUGIN " + resp.pluginName);
      }

      if (resp.hasCapabilityFlag(Flags.CLIENT_CONNECT_ATTRS)) {
        resp.clientAttributesLen = BytesUtil.getLenEncInt(in);
        resp.clientAttributes = BytesUtil.getEopStr(in);
      }

    } else {
      System.out.println("NO 41");
      resp.capabilityFlags = BytesUtil.getFixedLong(in, 2);
      System.out.println("Capability Flag " + resp.capabilityFlags);
      resp.maxPacketSize = BytesUtil.getFixedLong(in, 3);
      System.out.println("MAX PACK SIZE " + resp.maxPacketSize);
      resp.username = BytesUtil.getNullStr(in);

      if (resp.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB)) {
        resp.authResponse = BytesUtil.getNullStr(in);
        resp.schema = BytesUtil.getNullStr(in);
      } else {
        resp.authResponse = BytesUtil.getEopStr(in);
      }
    }
    System.out.println("USERNAME " + resp.username);
    System.out.println("AUTH RESP " + resp.authResponse);

    return resp;
  }
}
