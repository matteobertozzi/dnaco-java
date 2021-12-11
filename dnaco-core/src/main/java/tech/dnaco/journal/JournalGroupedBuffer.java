package tech.dnaco.journal;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.journal.JournalAsyncWriter.JournalEntryWriter;
import tech.dnaco.strings.StringUtil;

public class JournalGroupedBuffer<T extends JournalEntry> implements JournalBuffer<T> {
  private final HashMap<String, LogGroup<T>> groups = new HashMap<>(64);
  private final PagedByteArray buffer = new PagedByteArray(1 << 20);
  private final JournalEntryWriter<T> writer;
  private final Thread thread;

  private LogGroup<T> lastGroup = null;

  public JournalGroupedBuffer(final JournalEntryWriter<T> writer) {
    this.writer = writer;
    this.thread = Thread.currentThread();
  }

  public Thread getThread() {
    return thread;
  }

  public Collection<LogGroup<T>> getGroups() {
    return groups.values();
  }

  @Override
  public Set<String> getGroupIds() {
    return groups.keySet();
  }

  @Override
  public int size() {
    return buffer.size();
  }

  @Override
  public void flush() {
    // no-op
  }

  @Override
  public int add(final T entry) {
    final LogGroup<T> group = computeIfAbsentLogGroup(entry.getGroupId());
    group.add(buffer, writer, entry);
    return buffer.size();
  }

  @Override
  public void process(final String groupId, final LogBufferEntryProcessor processor)
      throws IOException {
    final LogGroup<T> group = groups.get(groupId);
    if (group != null) group.process(buffer, processor);
  }

  @Override
  public boolean hasGroupId(final String groupId) {
    return groups.containsKey(groupId);
  }

  private LogGroup<T> computeIfAbsentLogGroup(final String groupId) {
    if (lastGroup != null && lastGroup.getGroupId().equals(groupId)) {
      return lastGroup;
    }
    lastGroup = groups.computeIfAbsent(groupId, LogGroup::new);
    return lastGroup;
  }

  public static final class LogGroup<T extends JournalEntry> {
    private static final int ENTRY_OFFSET_EOF = 0xffffffff;

    private final String groupId;
    private int head = -1;
    private int tail = -1;

    private LogGroup(final String groupId) {
      if (StringUtil.isEmpty(groupId)) {
        throw new IllegalArgumentException("invalid empty groupId");
      }
      this.groupId = groupId;
    }

    public String getGroupId() {
      return groupId;
    }

    private void add(final PagedByteArray buffer, final JournalEntryWriter<T> writer, final T entry) {
      final int offset = buffer.size();

      // update the previous entry with the next (offset) pointer
      if (this.tail < 0) {
        this.head = offset;
      } else {
        buffer.setFixed32(tail, offset);
      }
      this.tail = offset;

      // write the new entry: | u32 next (EOF) | entry data... |
      buffer.addFixed32(ENTRY_OFFSET_EOF);
      writer.writeEntry(buffer, entry);
    }

    public void process(final PagedByteArray buffer, final LogBufferEntryProcessor processor)
        throws IOException {
      int lastOffset = head;
      int offset = head;
      while (offset != ENTRY_OFFSET_EOF) {
        lastOffset = offset;

        final int next = buffer.getFixed32(offset);
        processor.process(buffer, offset + 4);

        offset = next;
      }

      if (lastOffset != tail) {
        System.err.println("unexpected tail: lastOffset=" + lastOffset + " tail=" + tail + " offset=" + offset);
      }
    }
  }
}
