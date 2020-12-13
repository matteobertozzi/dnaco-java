package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public class Column extends MySqlPacket {
  public String catalog = "def";
  public String schema = "";
  public String table = "";
  public String org_table = "";
  public String name = "";
  public String org_name = "";
  public long characterSet = 0;
  public long columnLength = 16;
  public long type = Flags.MYSQL_TYPE_VAR_STRING;
  public long flags = 0;
  public long decimals = 0;

  public Column setFlags(final int flags) {
    this.flags = flags;
    return this;
  }

  public Column(final String name) {
    super(0);
    // Set this up by default. Allow overrides if needed
    this.characterSet = ResultSet.characterSet;
    this.name = name;
  }

  public Column(final String database, final String table, final String name) {
    super(0);
    // Set this up by default. Allow overrides if needed
    this.characterSet = ResultSet.characterSet;
    this.schema = database;
    this.table = table;
    this.org_table = table;
    this.name = name;
    this.org_name = name;
  }

  @Override
  public void write(final ByteBuf out) {
      final int hasCapabilities = Flags.CLIENT_PROTOCOL_41;
      if ((hasCapabilities & Flags.CLIENT_PROTOCOL_41) == Flags.CLIENT_PROTOCOL_41) {
        BytesUtil.writeLenEncStr(out, this.catalog);
        BytesUtil.writeLenEncStr(out, this.schema);
        BytesUtil.writeLenEncStr(out, this.table);
        BytesUtil.writeLenEncStr(out, this.org_table);
        BytesUtil.writeLenEncStr(out, this.name);
        BytesUtil.writeLenEncStr(out, this.org_name);
        BytesUtil.writeFiller(out, 1, (byte)0x0c);
        BytesUtil.writeFixedInt(out, 2, this.characterSet);
        BytesUtil.writeFixedInt(out, 4, this.columnLength);
        BytesUtil.writeFixedInt(out, 1, this.type);
        BytesUtil.writeFixedInt(out, 2, this.flags);
        BytesUtil.writeFixedInt(out, 1, this.decimals);
        BytesUtil.writeFiller(out, 2);
      } else {
        BytesUtil.writeLenEncStr(out, this.table);
        BytesUtil.writeLenEncStr(out, this.name);
        BytesUtil.writeFiller(out, 10);

        out.writeByte(3);
        BytesUtil.writeFixedInt(out, 3, this.columnLength);
        out.writeByte(1);
        BytesUtil.writeFixedInt(out, 1, this.type);
        out.writeByte(3);
        BytesUtil.writeFixedInt(out, 2, this.flags);
        BytesUtil.writeFixedInt(out, 1, this.decimals);
      }
  }

  public Column setTable(final String table) {
    this.table = table;
    return this;
  }

  public Column setType(final int type) {
    this.type = type;
    return this;
  }
}
