package tech.dnaco.storage;

import io.netty.buffer.ByteBuf;

public interface StorageCounter {
  long incrementAndGet(String tenantId, String key);
  long incrementAndGet(byte[] tenantId, byte[] key);
  long incrementAndGet(ByteBuf tenantId, ByteBuf key);

  long addAndGet(String tenantId, String key, long delta);
  long addAndGet(byte[] tenantId, byte[] key, long delta);
  long addAndGet(ByteBuf tenantId, ByteBuf key, long delta);
}