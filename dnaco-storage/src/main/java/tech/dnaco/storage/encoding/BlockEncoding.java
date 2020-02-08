/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.storage.encoding;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.compression.ZstdUtil;
import tech.dnaco.hash.HashUtil;
import tech.dnaco.io.ByteBufferInputStream;
import tech.dnaco.strings.HumansUtil;

public final class BlockEncoding {
  private static final String[] HASH_ALGO = new String[] { null, "SHA-512", "SHA3-512" };
  private static final int[] HASH_LENGTH = new int[] { 0, 64, 64 };
  private static final int HASH_NONE = 0;
  private static final int HASH_SHA_512 = 1;
  private static final int HASH_SHA3_512 = 2;

  private static final int DEFAULT_HASH = HASH_SHA_512;

  private BlockEncoding() {
    // no-op
  }

  public static int encode(final OutputStream stream, final ByteArrayWriter writer) throws IOException {
    return encode(stream, writer.rawBuffer(), writer.offset(), writer.writeOffset());
  }

  public static int encode(final OutputStream stream,
      final byte[] block, final int blockOff, final int blockSize) throws IOException {
    final byte[] hash = DEFAULT_HASH > 0 ? HashUtil.hash(HASH_ALGO[DEFAULT_HASH], block, blockOff, blockSize) : null;
    final ByteArraySlice zBlock = ZstdUtil.compress(block, blockOff, blockSize);

    int size = IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(stream, zBlock.length());
    size += IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(stream, blockSize);
    size += 1 + BytesUtil.length(hash) + zBlock.length();
    stream.write(DEFAULT_HASH);
    if (hash != null) stream.write(hash);
    stream.write(zBlock.rawBuffer(), zBlock.offset(), zBlock.length());

    if (false) {
      System.out.println(String.format("PLAIN BLOCK SIZE %s ZSTD %s %.2f%% - FLUSH SIZE %s",
        HumansUtil.humanSize(blockSize), HumansUtil.humanSize(zBlock.length()),
        1.0f - ((float)zBlock.length() / blockSize),
        HumansUtil.humanSize(size)));
    }

    return size;
  }

  public static byte[] decode(final ByteBufferInputStream stream) throws IOException {
    final int zBlockLen = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    final int blockLen = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    final int hashId = stream.read();
    System.out.println(" -> ZBLK " + zBlockLen + " BLK " + blockLen + " HASH " + hashId);
    final byte[] hash = hashId > 0 ? stream.readNBytes(HASH_LENGTH[hashId]) : null;
    final byte[] zBlock = stream.readNBytes(zBlockLen);
    final byte[] block = new byte[blockLen];

    // decompress
    final int zDecLen = ZstdUtil.decompress(block, zBlock, 0, zBlockLen);
    if (zDecLen != blockLen) {
      throw new IOException("Invalid decompression size. Expected " + blockLen + " got " + zDecLen);
    }

    // verify checksum
    final byte[] xHash = hashId > 0 ? HashUtil.hash(HASH_ALGO[hashId], block, 0, blockLen) : null;
    if (!BytesUtil.equals(xHash, hash)) {
      throw new IOException("Checksum Mismatch. Expected " + BytesUtil.toHexString(xHash) + " got " + BytesUtil.toHexString(hash));
    }

    return block;
  }
}
