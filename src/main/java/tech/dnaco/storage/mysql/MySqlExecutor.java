package tech.dnaco.storage.mysql;

import tech.dnaco.storage.mysql.protocol.AuthResponse;
import tech.dnaco.storage.mysql.protocol.AuthScramble;
import tech.dnaco.storage.mysql.protocol.Column;
import tech.dnaco.storage.mysql.protocol.ComKill;
import tech.dnaco.storage.mysql.protocol.ComPing;
import tech.dnaco.storage.mysql.protocol.ComQuery;
import tech.dnaco.storage.mysql.protocol.ComQuit;
import tech.dnaco.storage.mysql.protocol.Flags;
import tech.dnaco.storage.mysql.protocol.HandshakeResponse;
import tech.dnaco.storage.mysql.protocol.MySqlPacket;
import tech.dnaco.storage.mysql.protocol.OkResponse;
import tech.dnaco.storage.mysql.protocol.ResultSet;
import tech.dnaco.storage.mysql.protocol.Row;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MySqlExecutor extends SimpleChannelInboundHandler<MySqlPacket> {
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final MySqlPacket packet) throws Exception {
    System.err.println("HANDLE PACKET " + packet);
    if (packet instanceof ComQuery) {
      executeQuery(ctx, (ComQuery)packet);
    } else if (packet instanceof ComKill) {
      executeKill(ctx, (ComKill)packet);
    } else if (packet instanceof ComPing) {
      executePing(ctx, (ComPing)packet);
    } else if (packet instanceof AuthResponse) {
      handleAuthResponse(ctx, (AuthResponse)packet);
    } else if (packet instanceof HandshakeResponse) {
      handleHandshakeResponse(ctx, (HandshakeResponse)packet);
    } else if (packet instanceof ComQuit) {
      handleQuit(ctx, (ComQuit)packet);
    } else {
      System.err.println("TODO unhandled packet " + packet);
    }
  }

  private void handleHandshakeResponse(final ChannelHandlerContext ctx, final HandshakeResponse packet) {
    ctx.writeAndFlush(new AuthScramble(packet.getSequenceId() + 1)); // the native auth has en extra step...
    //ctx.writeAndFlush(new AuthSwitchRequest(sequenceId++));
    //ctx.writeAndFlush(new OkResponse(sequenceId++).setStatusFlags(Flags.SERVER_STATUS_AUTOCOMMIT));
  }

  private void handleAuthResponse(final ChannelHandlerContext ctx, final AuthResponse packet) {
    ctx.writeAndFlush(new OkResponse(packet.getSequenceId() + 1).setStatusFlags(Flags.SERVER_STATUS_AUTOCOMMIT));
  }

  private void handleQuit(final ChannelHandlerContext ctx, final ComQuit packet) {
    ctx.writeAndFlush(new OkResponse(packet.getSequenceId() + 1));
    ctx.close();
  }

  private void executePing(final ChannelHandlerContext ctx, final ComPing ping) {
    ctx.writeAndFlush(new OkResponse(ping.getSequenceId() + 1));
  }

  private void executeKill(final ChannelHandlerContext ctx, final ComKill packet) {
    ctx.writeAndFlush(new OkResponse(packet.getSequenceId() + 1));
  }

  private void executeQuery(final ChannelHandlerContext ctx, final ComQuery query) {
    System.out.println("Executing Query: " + query.getQuery());
    int sequenceId = query.getSequenceId() + 1;
    if (query.getQuery().equalsIgnoreCase("show variables")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Variable_name"));
      rs.addColumn(new Column("Value"));
      rs.addRow(new Row().addData("a").addData("1"));
      rs.addRow(new Row().addData("b").addData("2"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("select @@version_comment limit 1")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("@@version_comment"));
      rs.addRow(new Row().addData("thz-rulez"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("SELECT @@global.max_allowed_packet")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("@@global.max_allowed_packet"));
      rs.addRow(new Row().addData("4194304"));
      rs.write(ctx);
    } else if (query.getQuery().startsWith("SET NAMES")) {
      ctx.writeAndFlush(new OkResponse(sequenceId++));
    } else if (query.getQuery().equalsIgnoreCase("SHOW DATABASES")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Database"));
      rs.addRow(new Row().addData("foo"));
      rs.write(ctx);
    } else if (query.getQuery().startsWith("KILL ") || query.getQuery().equalsIgnoreCase("USE `foo`")) {
      ctx.writeAndFlush(new OkResponse(sequenceId++));
    } else if (query.getQuery().equalsIgnoreCase("SHOW VARIABLES LIKE 'character_set_database'")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Variable_name"));
      rs.addColumn(new Column("Value"));
      rs.addRow(new Row().addData("character_set_database").addData("latin1"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("SHOW /*!50002 FULL*/ TABLES")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Table_name"));
      rs.addColumn(new Column("Table_Type"));
      rs.addRow(new Row().addData("xyz").addData("BASE_TABLE"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("SELECT * FROM information_schema.routines WHERE routine_schema = 'foo' ORDER BY routine_name")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("SPECIFIC_NAME"));
      rs.addColumn(new Column("ROUTING_CATALOG"));
      rs.addColumn(new Column("ROUTINE_SCHEMA"));
      rs.addColumn(new Column("ROUTINE_NAME"));
      rs.addColumn(new Column("ROUTINE_TYPE"));
      rs.addColumn(new Column("DATA_TYPE"));
      rs.addColumn(new Column("CHARACTER_MAXIMUM_LENGTH"));
      rs.addColumn(new Column("CHARACTER_OCTET_LENGTH"));
      rs.addColumn(new Column("NUMERIC_PRECISION"));
      rs.addColumn(new Column("NUMERIC_SCALE"));
      rs.addColumn(new Column("DATETIME_PRECISION"));
      rs.addColumn(new Column("CHARACTER_SET_NAME"));
      rs.addColumn(new Column("COLLATION_NAME"));
      rs.addColumn(new Column("DTD_IDENTIFIER"));
      rs.addColumn(new Column("ROUTINE_BODY"));
      rs.addColumn(new Column("ROUTINE_DEFINITION"));
      rs.addColumn(new Column("EXTERNAL_NAME"));
      rs.addColumn(new Column("EXTERNAL_LANGUAGE"));
      rs.addColumn(new Column("PARAMETER_STYLE"));
      rs.addColumn(new Column("IS_DETERMINISTIC"));
      rs.addColumn(new Column("SQL_DATA_ACCESS"));
      rs.addColumn(new Column("SQL_PATH"));
      rs.addColumn(new Column("SECURITY_TYPE"));
      rs.addColumn(new Column("CREATED"));
      rs.addColumn(new Column("LAST_ALTERED"));
      rs.addColumn(new Column("SQL_MODE"));
      rs.addColumn(new Column("ROUTINE_COMMENT"));
      rs.addColumn(new Column("DEFINER"));
      rs.addColumn(new Column("CHARACTER_SET_CLIENT"));
      rs.addColumn(new Column("COLLATION_CONNECTION"));
      rs.addColumn(new Column("DATABASE_COLLATION"));
      rs.write(ctx);
    } else if (query.getQuery().equals("SELECT SPECIFIC_NAME, ROUTINE_TYPE, DTD_IDENTIFIER, IS_DETERMINISTIC, SQL_DATA_ACCESS, SECURITY_TYPE, DEFINER FROM `information_schema`.`ROUTINES` WHERE `ROUTINE_SCHEMA` = 'foo'")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("SPECIFIC_NAME"));
      rs.addColumn(new Column("ROUTINE_TYPE"));
      rs.addColumn(new Column("DTD_IDENTIFIER"));
      rs.addColumn(new Column("IS_DETERMINISTIC"));
      rs.addColumn(new Column("SQL_DATA_ACCESS"));
      rs.addColumn(new Column("SECURITY_TYPE"));
      rs.addColumn(new Column("DEFINER"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("SHOW TABLE STATUS LIKE 'xyz'")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Name"));
      rs.addColumn(new Column("Engine"));
      rs.addColumn(new Column("Version"));
      rs.addColumn(new Column("Row_format"));
      rs.addColumn(new Column("Rows"));
      rs.addColumn(new Column("Avg_row_length"));
      rs.addColumn(new Column("Data_length"));
      rs.addColumn(new Column("Max_data_length"));
      rs.addColumn(new Column("Index_length"));
      rs.addColumn(new Column("Data_free"));
      rs.addColumn(new Column("Auto_increment"));
      rs.addColumn(new Column("Create_time"));
      rs.addColumn(new Column("Update_time"));
      rs.addColumn(new Column("Collation"));
      rs.addColumn(new Column("Checksum"));
      rs.addColumn(new Column("Create_options"));
      rs.addColumn(new Column("Comment"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("SHOW CREATE TABLE `xyz`")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Table"));
      rs.addColumn(new Column("Create Table"));
      rs.addRow(new Row().addData("xyz").addData("CREATE TABLE xyz (thz_a varchar(16) NULL PRIMARY KEY, thz_b varchar(16))"));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("SELECT COUNT(1) FROM `xyz`")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Count"));
      rs.addRow(new Row().addData("2"));
      rs.write(ctx);
    } else if (query.getQuery().startsWith("SELECT COUNT(1) FROM `foo`.`xyz` WHERE (`thz_a`")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Count"));
      rs.addRow(new Row().addData("1"));
      rs.write(ctx);
    } else if (query.getQuery().startsWith("SHOW FULL COLUMNS FROM") ||
               query.getQuery().equalsIgnoreCase("SHOW COLUMNS FROM `foo`.`xyz`")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("Field"));
      rs.addColumn(new Column("Type"));
      rs.addColumn(new Column("Collation"));
      rs.addColumn(new Column("Null"));
      rs.addColumn(new Column("Key"));
      rs.addColumn(new Column("Default"));
      rs.addColumn(new Column("Extra"));
      rs.addColumn(new Column("Privileges"));
      rs.addColumn(new Column("Comments"));
      rs.addRow(new Row().addData("thz_a").addData("varchar(16)").addData("utf8_general_ci").addData("NO").addData("PRI").addData("NULL").addData("").addData("select,insert,update,references").addData(""));
      rs.addRow(new Row().addData("thz_b").addData("varchar(16)").addData("utf8_general_ci").addData("YES").addData("").addData("NULL").addData("").addData("select,insert,update,references").addData(""));
      rs.write(ctx);
    } else if (query.getQuery().equalsIgnoreCase("select * from xyz")) {
      final ResultSet rs = new ResultSet(sequenceId);
      rs.addColumn(new Column("foo", "xyz", "thz_a").setType(Flags.MYSQL_TYPE_VAR_STRING).setFlags(Flags.PRI_KEY_FLAG & Flags.NOT_NULL_FLAG));
      rs.addColumn(new Column("foo", "xyz", "thz_b").setType(Flags.MYSQL_TYPE_VAR_STRING));
      rs.addRow(new Row().addData("10").addData("data a"));
      rs.addRow(new Row().addData("20").addData("data b"));
      rs.write(ctx);
    } else if (query.getQuery().startsWith("UPDATE ")) {
      ctx.writeAndFlush(new OkResponse(sequenceId++).setAffectedRows(1));
    } else {
      ctx.writeAndFlush(new OkResponse(sequenceId++).setAffectedRows(1));
    }
  }
}
