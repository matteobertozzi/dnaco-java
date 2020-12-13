package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class Eof extends MySqlPacket {
  public long statusFlags = 0;
  public long warnings = 0;

  public Eof(final int seqId) {
    super(seqId);
  }

  public void setStatusFlag(final long flag) {
      this.statusFlags |= flag;
  }

  public void removeStatusFlag(final long flag) {
      this.statusFlags &= ~flag;
  }

  public void toggleStatusFlag(final long flag) {
      this.statusFlags ^= flag;
  }

  public boolean hasStatusFlag(final long flag) {
      return ((this.statusFlags & flag) == flag);
  }

  @Override
  public void write(final ByteBuf out) {
    out.writeByte(Flags.EOF);
    BytesUtil.writeFixedInt(out, 2, this.warnings);
    BytesUtil.writeFixedInt(out, 2, this.statusFlags);
  }
}
