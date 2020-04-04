package tech.dnaco.storage.client;

import java.util.TreeMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;
import tech.dnaco.storage.StorageException;
import tech.dnaco.storage.StorageKeyExistsException;
import tech.dnaco.storage.StorageReadOnlyTransaction;
import tech.dnaco.storage.StorageTransaction;
import tech.dnaco.storage.client.memory.MemoryClient;
import tech.dnaco.storage.entity.StorageEntity;

public class TestMemoryClient {
  @Test
  public void testCounters() {
    final MemoryClient client = new MemoryClient();
    Assertions.assertEquals(1, client.incrementAndGet("tenant-1", "/key-1"));
    Assertions.assertEquals(1, client.incrementAndGet("tenant-2", "/key-1"));
    Assertions.assertEquals(2, client.incrementAndGet("tenant-1", "/key-1"));
    Assertions.assertEquals(2, client.incrementAndGet("tenant-2", "/key-1"));
    Assertions.assertEquals(5, client.addAndGet("tenant-1", "/key-1", 3));
    Assertions.assertEquals(6, client.addAndGet("tenant-2", "/key-1", 4));
  }

  @Test
  public void testKeyValues() throws StorageException {
    final MemoryClient client = new MemoryClient();
    try (StorageTransaction txn = client.newTransaction("tenant-1")) {
      txn.insert("/k1", new TestEntity("v1"));
      Assertions.assertEquals("v1", txn.get("/k1", TestEntity.class).value);

      try {
        txn.insert("/k1", new TestEntity("v2"));
        Assertions.fail("unexpected insert of existing key: /k1");
      } catch (StorageKeyExistsException e) {
        // expected
      }

      txn.update("/k1", new TestEntity("v2"));
      Assertions.assertEquals("v2", txn.get("/k1", TestEntity.class).value);

      txn.commit();
    }

    try (StorageReadOnlyTransaction txn = client.newReadOnlyTransaction("tenant-1")) {
      final TestEntity entry = txn.get("/k1", TestEntity.class);
      Assertions.assertEquals("v2", entry.value);
    }
  }

  @Test
  public void testScan() {
    final Object obj = new Object();
    final TreeMap<String, Object> tree = new TreeMap<>();
    tree.put("/aaa", obj);
    tree.put("/aaa/1", obj);
    tree.put("/aaa/2", obj);
    tree.put("/aaa/3", obj);
    tree.put("/bbb", obj);
    tree.put("/bbb/zzz/1", obj);
    tree.put("/bbb/zzz/2", obj);
    tree.put("/bbb/zzz/3", obj);

    for (String entry: tree.tailMap("/aaa", true).keySet()) {
      if (!entry.startsWith("/aaa")) break;
      System.out.println(entry);
    }

    scanTest((x) -> System.out.println(x));
  }

  public void scanTest(Consumer<String> supplier) {
    supplier.accept("a");
    supplier.accept("b");
  }

  private static final class TestEntity implements StorageEntity {
    private String value;

    public TestEntity(final String value) {
      this.value = value;
    }
  }
}