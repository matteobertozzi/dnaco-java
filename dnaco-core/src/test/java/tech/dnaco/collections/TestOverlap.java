/*
 *   A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
 * 1                                               +---+
 * 2 +-------+   +-----+         +-------+   +---+
 * 3     +---------+     +-----+
 * 4                 +-----+       +-------+
 */

package tech.dnaco.collections;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

public class TestOverlap {
  public static void main(final String[] args) {
    List<BlockInfo> fileA = Arrays.asList(new BlockInfo(1, "A", "C"), new BlockInfo(1, "E", "F"), new BlockInfo(1, "H", "L"));
    List<BlockInfo> fileB = Arrays.asList(new BlockInfo(2, "B", "D"), new BlockInfo(2, "H", "I"));
    List<BlockInfo> fileC = Arrays.asList(new BlockInfo(3, "A", "D"), new BlockInfo(3, "M", "N"));
    List<BlockInfo> fileD = Arrays.asList(new BlockInfo(4, "X", "Z"));

    System.out.println(fileA.get(0).isOverlapping(fileB.get(0)));
    System.out.println(fileB.get(0).isOverlapping(fileA.get(0)));
    System.out.println(fileB.get(0).isOverlapping(fileC.get(0)));

    final PriorityQueue<BlockInfo> blocks = new PriorityQueue<>();
    blocks.addAll(fileA);
    blocks.addAll(fileB);
    blocks.addAll(fileC);
    blocks.addAll(fileD);
    System.out.println(blocks);
    mergeIntervals(Arrays.asList(fileA, fileB, fileC, fileD));
  }

  public static void mergeIntervals(List<List<BlockInfo>> blockIndexes) {
    final PriorityQueue<BlockInfo> blocks = new PriorityQueue<>();
    for (List<BlockInfo> index: blockIndexes) blocks.addAll(index);

    HashSet<Integer> merged = new HashSet<>();
    HashSet<Integer> overlapping = new HashSet<>();
    BlockInfo refEntry = blocks.poll();
    overlapping.add(refEntry.fileId);
    while (!blocks.isEmpty()) {
      BlockInfo entry = blocks.poll();
      if (!refEntry.isOverlapping(entry)) {
        if (!merged.containsAll(overlapping)) {
          System.out.println(overlapping);
        }
        overlapping.clear();
        merged.addAll(overlapping);
        refEntry = entry;
      }
      overlapping.add(entry.fileId);
    }
    System.out.println(overlapping);
  }

  public static class BlockInfo implements Comparable<BlockInfo> {
    private final byte[] firstKey;
    private final byte[] lastKey;
    private final int fileId;

    public BlockInfo(final int fileId, final String firstKey, final String lastKey) {
      this.firstKey = firstKey.getBytes();
      this.lastKey = lastKey.getBytes();
      this.fileId = fileId;
    }

    public boolean isOverlapping(final BlockInfo other) {
      // x1 <= C <= x2 && y1 <= C <= y2
      // x1 <= y2 && y1 <= x2
      return Arrays.compare(firstKey, other.lastKey) <= 0 &&
             Arrays.compare(other.firstKey, lastKey) <= 0;
    }

    @Override
    public int compareTo(final BlockInfo o) {
      return Arrays.compare(firstKey, o.firstKey);
    }

    @Override
    public String toString() {
      return "BlockInfo [fileId=" + fileId +
             ", firstKey=" + new String(firstKey) +
             ", lastKey=" + new String(lastKey) + "]";
    }
  }

}