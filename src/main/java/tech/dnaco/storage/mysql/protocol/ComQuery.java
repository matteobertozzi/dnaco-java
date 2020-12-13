package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class ComQuery extends MySqlPacket {
  private String query;

  public ComQuery(final int sequenceId) {
    super(sequenceId);
  }

  public String getQuery() {
    return query;
  }

  @Override
  public void write(final ByteBuf out) {
    BytesUtil.writeFixedStr(out, query.length(), query);
  }

  public static ComQuery read(final ByteBuf in, final int sequenceId) {
    final ComQuery com = new ComQuery(sequenceId);
    com.query = BytesUtil.getEopStr(in);
    return com;
  }
}
