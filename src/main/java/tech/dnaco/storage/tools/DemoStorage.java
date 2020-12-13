package tech.dnaco.storage.tools;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.gullivernet.commons.database.connection.DbConnection;
import com.gullivernet.commons.database.connection.DbConnectionProvider;
import com.gullivernet.commons.database.connection.DbInfo;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.api.Storage;
import tech.dnaco.storage.blocks.BlockEntry;
import tech.dnaco.storage.blocks.BlockEntryMergeIterator.BlockEntryMergeOptions;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;

public class DemoStorage {
  private static void testWrite(final Storage storage, final int startKey, final int stopKey) throws IOException {
    testWrite(storage, startKey, stopKey, 1);
  }

  private static void testWrite(final Storage storage, final int startKey, final int stopKey, final int stepKey) throws IOException {
    if (true) {
      final long prepareTime = System.nanoTime();
      long seqId = 2_000_000;
      final List<BlockEntry> entries = new ArrayList<>(1_000_000);
      for (int i = startKey; i < stopKey; i += stepKey) {
        final byte[] value = String.format("%020d", i).getBytes();
        final byte[] key = new RowKeyBuilder().add("foo").add(value).drain();
        final BlockEntry entry = new BlockEntry()
          .setTimestamp(0)
          .setSeqId(seqId++)
          .setFlags(1)
          .setKey(new ByteArraySlice(key))
          //.setValue(new ByteArraySlice(value));
          .setValue(new ByteArraySlice(("x" + i).getBytes()));
        entries.add(entry);
      }
      Logger.info("prepare time: {}", HumansUtil.humanTimeSince(prepareTime));

      storage.addAll(entries);
      storage.flush();
    }
  }

  public static void main(final String[] args) throws Exception {

    final String url = "jdbc:mysql://127.0.0.1:3320/mdc_lega_seriea?user=legaseriea&password=p0AD6G6KaP8KCe0QSB3X&autoReconnect=true&useSSL=false&autoReconnect=true&useServerPrepStmts=false&rewriteBatchedStatements=true&useCompression=true";
    try (DbConnection con = new DbConnectionProvider().getConnection(DbInfo.fromUrl(url), "test")) {
      final Storage storage = Storage.getInstance("LEGA_TABGEN");
      try (PreparedStatement stmt = con.prepareStreamingStatement("tabgen", "SELECT * FROM tabgen")) {
        try (ResultSet rs = stmt.executeQuery()) {
          final int index = 0;
          while (rs.next()) {
            final String tabname = rs.getString("tabname");
            final String reckey = rs.getString("reckey");
            final String syncIdAgente = rs.getString("sync_idagente");
            for (int i = 1; i <= 20; ++i) {
              final String fieldName = String.format("val%02d", i);
              final String value = StringUtil.emptyIfNull(rs.getString(fieldName));
              storage.addAll(List.of(new BlockEntry()
                .setSeqId(0)
                .setKey(new ByteArraySlice(new RowKeyBuilder().add(tabname).add(reckey).add(syncIdAgente).add(fieldName).drain()))
                .setValue(new ByteArraySlice(value.getBytes()))));
            }
          }
        }
      }
      storage.flush();
    }
  }

  private static void foo() throws Exception {
    final Storage storage = Storage.getInstance("DEMO_STORAGE");

    //testWrite(storage, 1, 10);
    storage.addAll(List.of(
      new BlockEntry()
        .setSeqId(3000000)
        .setKey(new ByteArraySlice(new RowKeyBuilder().add("entity").add("kay1").add("column").drain()))
        .setValue(new ByteArraySlice("MemZtore".getBytes())),
      new BlockEntry()
        .setSeqId(3000001)
        .setKey(new ByteArraySlice(new RowKeyBuilder().add("entity").add("kay2").add("column").drain()))
        .setValue(new ByteArraySlice("MemZtore2".getBytes()))
    ));

    System.out.println("KAY0: " + storage.getBlockManager().hasKey(new RowKeyBuilder().add("entity").add("kay0").drain()));
    System.out.println("KAY1: " + storage.getBlockManager().hasKey(new RowKeyBuilder().add("entity").add("kay1").drain()));
    System.out.println("KAY2: " + storage.getBlockManager().hasKey(new RowKeyBuilder().add("entity").add("kay2").drain()));
    System.out.println("KAY2: " + storage.getBlockManager().hasKey(new RowKeyBuilder().add("entity").add("kay3").drain()));

    if (false) {
      System.out.println("------ SCAN FROM ------");
      final byte[] value = String.format("%020d", 5).getBytes();
      final byte[] key = new RowKeyBuilder().add("foo").add(value).drain();
      final ByteArraySlice stopKey = new ByteArraySlice(new RowKeyBuilder().add("foo").add(String.format("%20d", 6)).drain());
      storage.getBlockManager().scanFrom(new BlockEntryMergeOptions(), new ByteArraySlice(key), (entry) -> {
        if (entry.getKey().compareTo(stopKey) >= 0) return false;
        System.out.println(entry);
        return true;
      });
    }

    if (false) {
      System.out.println("------ SCAN ------");
      storage.getBlockManager().fullScan(new BlockEntryMergeOptions().setMaxVersions(1), (entry) -> {
        System.out.println(entry);
        return true;
      });
    }
    //System.out.println(TelemetryCollectorRegistry.INSTANCE.humanReport());
  }
}
