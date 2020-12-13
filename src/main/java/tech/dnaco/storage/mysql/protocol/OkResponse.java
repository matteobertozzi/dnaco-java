package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class OkResponse extends MySqlPacket {
  public long affectedRows = 0;
  public long lastInsertId = 0;
  public long statusFlags = 0;
  public long warnings = 0;

  public OkResponse(final int sequenceId) {
    super(sequenceId);
  }

  @Override
  public void write(final ByteBuf out) {
    out.writeByte(Flags.OK);
    BytesUtil.writeLenEncInt(out, this.affectedRows);
    BytesUtil.writeLenEncInt(out, this.lastInsertId);
    BytesUtil.writeFixedInt(out, 2, this.statusFlags);
    BytesUtil.writeFixedInt(out, 2, this.warnings);
  }

  public OkResponse setAffectedRows(final int affectedRows) {
    this.affectedRows = affectedRows;
    return this;
  }

  public OkResponse setStatusFlags(final int flags) {
    this.statusFlags = flags;
    return this;
  }
}
