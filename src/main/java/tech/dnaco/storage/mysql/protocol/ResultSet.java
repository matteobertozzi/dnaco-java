package tech.dnaco.storage.mysql.protocol;

import java.util.ArrayList;

import tech.dnaco.storage.mysql.MySqlPacketEncoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class ResultSet {
  public static long characterSet = 0;

  private final ArrayList<Column> columns = new ArrayList<>();
  private final ArrayList<Row> rows = new ArrayList<>();
  private final int sequenceId;

  public ResultSet(final int sequenceId) {
    this.sequenceId = sequenceId;
  }

  public void addColumn(final Column column) {
    this.columns.add(column);
  }

  public void addRow(final Row row) {
    this.rows.add(row);
  }

  public void write(final ChannelHandlerContext ctx) {
    int pkgSeqId = this.sequenceId;
    long maxRowSize = 8;

    final ByteBuf testBuf = ctx.alloc().heapBuffer();
    final int offset = testBuf.writerIndex();
    for (final Column col : this.columns) {
      MySqlPacketEncoder.writePacket(testBuf, col, 0);
      final long size = testBuf.writerIndex() - offset;
      testBuf.writerIndex(offset);
      if (size > maxRowSize) maxRowSize = size;
    }
    testBuf.release();

    final ColCount colCount = new ColCount(pkgSeqId++, this.columns.size());
    ctx.write(colCount);

    for (final Column col : this.columns) {
      col.columnLength = maxRowSize;
      col.setSequenceId(pkgSeqId++);
      ctx.write(col);
    }

    Eof eof = new Eof(pkgSeqId++);
    ctx.write(eof);

    //final ColCount rowCount = new ColCount(pkgSeqId++, this.rows.size());
    //ctx.write(rowCount);

    for (final Row row : this.rows) {
      row.setSequenceId(pkgSeqId++);
      ctx.write(row);
    }

    eof = new Eof(pkgSeqId++);
    ctx.writeAndFlush(eof);
  }

  private static final class ColCount extends MySqlPacket {
    private final int count;

    protected ColCount(final int sequenceId, final int count) {
      super(sequenceId);
      this.count = count;
    }

    @Override
    public void write(final ByteBuf out) {
      BytesUtil.writeLenEncInt(out, count);
    }
  }
}
