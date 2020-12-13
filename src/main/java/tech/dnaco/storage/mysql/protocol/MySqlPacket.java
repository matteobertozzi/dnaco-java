package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public abstract class MySqlPacket {
  private int sequenceId;

  protected MySqlPacket(final int sequenceId) {
    this.sequenceId = sequenceId;
  }

  public int getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(final int seqId) {
    this.sequenceId = seqId;
  }

  public abstract void write(final ByteBuf out);
}
