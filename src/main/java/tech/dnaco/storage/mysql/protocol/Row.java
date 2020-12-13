package tech.dnaco.storage.mysql.protocol;

import java.util.ArrayList;

import io.netty.buffer.ByteBuf;

public class Row extends MySqlPacket {
  public int type = Flags.ROW_TYPE_TEXT;
  public int colType = Flags.MYSQL_TYPE_VAR_STRING;
  public ArrayList<Object> data = new ArrayList<>();

  public Row() {
    super(0);
  }

  public Row addData(final String data) {
      this.data.add(data);
      return this;
  }

  public Row addData(final long data) {
      this.data.add(String.valueOf(data));
      return this;
  }

  public void addData(final float data) {
      this.data.add(String.valueOf(data));
  }

  public void addData(final boolean data) {
      this.data.add(String.valueOf(data));
  }

  // Add other addData for other types here

  @Override
  public void write(final ByteBuf out) {
    for (final Object obj: this.data) {
        switch (this.type) {
            case Flags.ROW_TYPE_TEXT:
                if (obj == null) {
                  out.writeByte(0xfb);
                } if (obj instanceof String) {
                  BytesUtil.writeLenEncStr(out, (String)obj);
                } else if (obj instanceof Integer) {
                  BytesUtil.writeLenEncInt(out, ((Integer)obj).intValue());
                } else if (obj instanceof Long) {
                  BytesUtil.writeLenEncInt(out, ((Long)obj).longValue());
                } else {
                    // trigger error
                  System.err.println("Type Not Supported " + obj.getClass());
                }
                break;
            case Flags.ROW_TYPE_BINARY:
                break;
            default:
                break;
        }
    }
  }
}
