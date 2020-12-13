package tech.dnaco.storage.mysql.protocol;

import io.netty.buffer.ByteBuf;

public final class BytesUtil {
  private BytesUtil() {
    // no-op
  }

  public static void writeFiller(final ByteBuf out, final int length) {
    writeFiller(out, length, (byte) 0x00);
  }

  public static void writeFiller(final ByteBuf out, final int length, final byte value) {
    out.ensureWritable(length);
    for (int i = 0; i < length; ++i) {
      out.writeByte(value);
    }
  }

  public static void writeFixedStr(final ByteBuf out, final int size, final String value) {
    out.ensureWritable(size);
    out.writeBytes(value.getBytes());
    for (int i = value.length(); i < size; ++i) {
      out.writeByte((byte) 0);
    }
  }

  public static void writeNullStr(final ByteBuf out, final String value) {
    writeFixedStr(out, 1 + value.length(), value);
  }

  public static void writeFixedInt(final ByteBuf out, final int length, final long value) {
    out.ensureWritable(length);
    for (int i = 0; i < length; ++i) {
      out.writeByte((byte) ((value >> (i << 3)) & 0xFF));
    }
  }

  public static int getFixedInt(final ByteBuf in, final int size) {
    int value = 0;
    for (int i = 0; i < size; ++i) {
      value |= (in.readByte() & 0xFF) << (i << 3);
    }
    return value;
  }

  public static long getFixedLong(final ByteBuf in, final int size) {
    long value = 0;
    for (int i = 0; i < size; ++i) {
      value |= (in.readByte() & 0xFF) << (i << 3);
    }
    return value;
  }

  public static void skipFiller(final ByteBuf in, final int size) {
    in.readerIndex(in.readerIndex() + size);
  }

  public static String getNullStr(final ByteBuf in) {
    final StringBuilder builder = new StringBuilder();
    byte c;
    while ((c = in.readByte()) > 0x00) {
      builder.append((char)c);
    }
    return builder.toString();
  }

  public static long getLenEncInt(final ByteBuf in) {
    int size = 0;
    final int v = in.readByte() & 0xFF;
    if (v < 251) {
      in.readerIndex(in.readerIndex() - 1);
      size = 1;
    } else if (v == 252) {
      size = 2;
    } else if (v == 253) {
      size = 3;
    } else if (v == 254) {
      size = 8;
    }
    return getFixedLong(in, size);
  }

  public static String getFixedStr(final ByteBuf in, final int length, final boolean base64) {
    final StringBuilder str = new StringBuilder(length);
    for (int i = 0; i < length; ++i) {
      str.append((char) in.readByte());
    }
    return str.toString();
  }

  public static String getEopStr(final ByteBuf in) {
    final StringBuilder str = new StringBuilder(in.readableBytes());
    while (in.readableBytes() > 0) {
      str.append((char)in.readByte());
    }
    return str.toString();
  }

  public static void writeLenEncInt(final ByteBuf out, final long value) {
    out.ensureWritable(9);
    if (value < 251) {
      out.writeByte((byte) ((value >> 0) & 0xFF));
    } else if (value < 65535) {
      out.writeByte((byte) 0xFC);
      out.writeByte((byte) ((value >> 0) & 0xFF));
      out.writeByte((byte) ((value >> 8) & 0xFF));
    } else if (value < 16777215) {
      out.writeByte((byte) 0xFD);
      out.writeByte((byte) ((value >> 0) & 0xFF));
      out.writeByte((byte) ((value >> 8) & 0xFF));
      out.writeByte((byte) ((value >> 16) & 0xFF));
    } else {
      out.writeByte((byte) 0xFE);
      out.writeByte((byte) ((value >> 0) & 0xFF));
      out.writeByte((byte) ((value >> 8) & 0xFF));
      out.writeByte((byte) ((value >> 16) & 0xFF));
      out.writeByte((byte) ((value >> 24) & 0xFF));
      out.writeByte((byte) ((value >> 32) & 0xFF));
      out.writeByte((byte) ((value >> 40) & 0xFF));
      out.writeByte((byte) ((value >> 48) & 0xFF));
      out.writeByte((byte) ((value >> 56) & 0xFF));
    }
  }

  public static void writeLenEncStr(final ByteBuf out, final String value) {
    out.ensureWritable(value.length());
    if (value.equals("")) {
      out.writeByte((byte)0x00);
      return;
    }

    writeLenEncInt(out, value.length());
    writeFixedStr(out, value.length(), value);
  }
}
