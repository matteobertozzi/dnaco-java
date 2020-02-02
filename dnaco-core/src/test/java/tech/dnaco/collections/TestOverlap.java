/*
 *   A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
 * 1                                               +---+
 * 2 +-------+   +-----+         +-------+   +---+
 * 3     +---------+     +-----+
 * 4                 +-----+       +-------+
 */

package tech.dnaco.collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import org.junit.jupiter.api.Test;

public class TestOverlap {
  public static void main(final String[] args) {
    List<BlockInfo> fileA = Arrays.asList(new BlockInfo(1, "A", "E"), new BlockInfo(1, "G", "J"), new BlockInfo(1, "O", "S"), new BlockInfo(1, "U", "W"));
    List<BlockInfo> fileB = Arrays.asList(new BlockInfo(2, "C", "H"), new BlockInfo(2, "K", "N"));
    List<BlockInfo> fileC = Arrays.asList(new BlockInfo(3, "I", "L"), new BlockInfo(3, "P", "T"));
    List<BlockInfo> fileD = Arrays.asList(new BlockInfo(4, "X", "Z"));

    System.out.println(fileA.get(0).isOverlapping(fileB.get(0)));
    System.out.println(fileB.get(0).isOverlapping(fileA.get(0)));
    System.out.println(fileB.get(0).isOverlapping(fileC.get(0)));

    final PriorityQueue<BlockInfo> blocks = new PriorityQueue<>();
    for (BlockInfo b: fileA) blocks.add(b);
    for (BlockInfo b: fileB) blocks.add(b);
    for (BlockInfo b: fileC) blocks.add(b);
    for (BlockInfo b: fileD) blocks.add(b);
    System.out.println(blocks);
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