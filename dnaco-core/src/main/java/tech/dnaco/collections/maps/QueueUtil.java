package tech.dnaco.collections.maps;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class QueueUtil {
  private QueueUtil() {
    // no-op
  }

  public static <T> boolean isEmpty(final Queue<T> queue) {
    return queue == null || queue.isEmpty();
  }

  public static <T> boolean isNotEmpty(final Queue<T> queue) {
    return queue != null && !queue.isEmpty();
  }

  public static <T> int size(final Queue<T> queue) {
    return queue != null ? queue.size() : 0;
  }

  public static <T> T pollWithoutInterrupt(final BlockingQueue<T> queue, final long timeout, final TimeUnit unit) {
    try {
      return queue.poll(timeout, unit);
    } catch (final InterruptedException e) {
      Thread.interrupted();
      return null;
    }
  }
}
