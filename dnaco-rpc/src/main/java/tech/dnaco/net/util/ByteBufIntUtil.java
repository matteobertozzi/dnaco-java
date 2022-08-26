package tech.dnaco.net.util;

import io.netty.buffer.ByteBuf;

public final class ByteBufIntUtil {
  private ByteBufIntUtil() {
    // no-op
  }

  public static void writeFixed(final ByteBuf buf, final long v, final int bytesWidth) {
    switch (bytesWidth) {
      case 8: buf.writeByte((int)((v >>> 56) & 0xff));
      case 7: buf.writeByte((int)((v >>> 48) & 0xff));
      case 6: buf.writeByte((int)((v >>> 40) & 0xff));
      case 5: buf.writeByte((int)((v >>> 32) & 0xff));
      case 4: buf.writeByte((int)((v >>> 24) & 0xff));
      case 3: buf.writeByte((int)((v >>> 16) & 0xff));
      case 2: buf.writeByte((int)((v >>>  8) & 0xff));
      case 1: buf.writeByte((int)((v) & 0xff));
    }
  }

  public static long readFixed(final ByteBuf buf, final int bytesWidth) {
    long result = 0;
    switch (bytesWidth) {
      case 8: result  = (((long)buf.readByte() & 0xff) << 56);
      case 7: result += (((long)buf.readByte() & 0xff) << 48);
      case 6: result += (((long)buf.readByte() & 0xff) << 40);
      case 5: result += (((long)buf.readByte() & 0xff) << 32);
      case 4: result += (((long)buf.readByte() & 0xff) << 24);
      case 3: result += (((long)buf.readByte() & 0xff) << 16);
      case 2: result += (((long)buf.readByte() & 0xff) <<  8);
      case 1: result += (((long)buf.readByte() & 0xff));
    }
    return result;
  }

  public static int writeVarLong(final ByteBuf buf, long v) {
    int length = 0;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buf.writeByte((int)((v & 0x7F) | 0x80));
      v >>>= 7;
      length++;
    }
    buf.writeByte((int)(v & 0x7F));
    return length + 1;
  }

  public static long readVarLong(final ByteBuf buf) {
    long value = 0;
    int shift = 0;
    long b;
    while (((b = buf.readByte()) & 0x80) != 0) {
      value |= (b & 0x7F) << shift;
      shift += 7;
    }
    return value | (b << shift);
  }
}
