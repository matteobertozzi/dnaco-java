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

package tech.dnaco.net.frame;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import tech.dnaco.strings.HumansUtil;

public class DnacoFrame extends AbstractReferenceCounted {
  public static final AtomicLong ALLOCATED = new AtomicLong();

  private ByteBuf data;
  private int header;

  // ===============================================================================================
  //  Alloc/Release related
  // ===============================================================================================
  private static final ArrayBlockingQueue<DnacoFrame> allocQueue = new ArrayBlockingQueue<>(64);

  public static DnacoFrame alloc(final int rev, final ByteBuf data) {
    DnacoFrame frame = DnacoFrame.allocQueue.poll();
    if (frame == null) frame = new DnacoFrame();

    frame.setFrame(rev, data);
    //System.out.println("ALLOC FRAME ");
    ALLOCATED.incrementAndGet();
    return frame;
  }

  @Override
  public void deallocate() {
    this.data.release();
    //System.out.println("DEALLOC FRAME");
    ALLOCATED.decrementAndGet();
    DnacoFrame.allocQueue.offer(this);
  }

  @Override
  public ReferenceCounted touch(final Object hint) {
    return this;
  }

  private void setFrame(final int rev, final ByteBuf data) {
    if (this.refCnt() == 0) this.setRefCnt(1);

    if (rev < 0 || rev >= 32) {
      throw new IllegalArgumentException("invalid frame revision " + rev + ", expected [0, 32[");
    }

    if (data == null) {
      throw new IllegalArgumentException("invalid frame with null data");
    }

    final int length = data.readableBytes();
    if (length < 1 || length > 0x7ffffff) {
      throw new IllegalArgumentException("invalid frame length " + HumansUtil.humanSize(length) + ", expected [1, 128MiB]");
    }

    this.header = (rev << 27) | (length - 1);
    this.data = data;
  }

  // ===============================================================================================
  public int getRev() {
    return readRev(header);
  }

  public int getLength() {
    return readDataLength(header);
  }

  public ByteBuf getData() {
    return data;
  }

  protected int getHeader() {
    return header;
  }

  public static int readRev(final int header) {
    return (int) ((header & 0xffffffffL) >> 27) & 0x1f;
  }

  public static int readDataLength(final int header) {
    return 1 + (header & 0x7ffffff);
  }

  @Override
  public String toString() {
    return "DnacoFrame [rev=" + getRev() + ", length=" + getLength() + "]";
  }
}
