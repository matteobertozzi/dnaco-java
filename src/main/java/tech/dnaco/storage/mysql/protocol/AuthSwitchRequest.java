package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class AuthSwitchRequest extends MySqlPacket {
  public String pluginName = "mysql_clear_password";
  public String authPluginData = "";

  public AuthSwitchRequest(final int seqId) {
    super(seqId);
  }

  @Override
  public void write(final ByteBuf out) {
    out.writeByte(Flags.EOF);
    BytesUtil.writeNullStr(out, this.pluginName);
    BytesUtil.writeNullStr(out, this.authPluginData);
  }
}
