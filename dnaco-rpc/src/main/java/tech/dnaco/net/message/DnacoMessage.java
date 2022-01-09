/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.net.message;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class DnacoMessage extends AbstractReferenceCounted {
  private DnacoMetadataMap metadataMap;
  private ByteBuf metadata;
  private ByteBuf data;
  private final long packetId;
  private final int metaCount;

  public DnacoMessage(final long packetId, final int metaCount, final ByteBuf metadata, final ByteBuf data) {
    this.setRefCnt(1);
    this.packetId = packetId;
    this.metaCount = metaCount;
    this.metadata = metadata;
    this.data = data;
  }

  public DnacoMessage(final long packetId, final DnacoMetadataMap metadata, final ByteBuf data) {
    this.setRefCnt(1);
    this.packetId = packetId;
    this.metaCount = (metadata != null) ? metadata.size() : 0;
    this.metadataMap = metadata;
    this.metadata = null;
    this.data = data;
  }

  public long packetId() {
    return packetId;
  }

  public boolean hasMetadata() {
    return metaCount > 0;
  }

  public int metadataCount() {
    return metaCount;
  }

  public ByteBuf data() {
    return data;
  }

  public DnacoMetadataMap metadataMap() {
    if (metadataMap != null) return metadataMap;

    metadataMap = DnacoMessageUtil.decodeMetadata(metadata, metaCount);
    return metadataMap;
  }

  // ===============================================================================================
  //  Metadata/Http related
  // ===============================================================================================
  public String method() {
    return metadataMap().get(DnacoMessageUtil.METADATA_FOR_HTTP_METHOD);
  }

  public String uri() {
    return metadataMap().get(DnacoMessageUtil.METADATA_FOR_HTTP_URI);
  }

  public int status() {
    return metadataMap().getInt(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, -1);
  }

  // ===============================================================================================
  //  Alloc/Release related
  // ===============================================================================================
  @Override
  public ReferenceCounted touch(final Object hint) {
    return this;
  }

  @Override
  public DnacoMessage retain() {
    super.retain();
    return this;
  }

  @Override
  protected void deallocate() {
    if (metadata != null) {
      metadata.release();
      metadata = null;
    }
    if (data != null) {
      data.release();
      data = null;
    }
    metadataMap = null;
  }
}
