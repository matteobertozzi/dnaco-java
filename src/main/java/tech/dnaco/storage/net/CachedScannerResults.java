package tech.dnaco.storage.net;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import tech.dnaco.data.CborFormat;
import tech.dnaco.io.FileUtil;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.net.models.JsonEntityDataRows;
import tech.dnaco.storage.net.models.ScanResult;

public class CachedScannerResults {
  private final String scannerId = UUID.randomUUID().toString();

  private JsonEntityDataRows.Builder rows;
  private EntitySchema schema;

  private ScanResult firstResult;
  private int chunkCount;
  private long chunkSize;
  private long rowsSize;
  private long rowCount;
  private long rowRead;

  public boolean isEmpty() {
    return chunkCount == 0;
  }

  public long getRowCount() {
    return rowCount;
  }

  public long getRowRead() {
    return rowRead;
  }

  public long getRowsSize() {
    return rowsSize;
  }

  public ScanResult getFirstResult() {
    return firstResult;
  }

  public void sealResults() throws IOException {
    sealResults(true);
  }

  public void sealResults(final boolean hasMore) throws IOException {
    if (rows == null || rows.isEmpty()) return;

    final ScanResult scanResult = new ScanResult(schema.getEntityName(), schema.getKeyFields(), hasMore, rows.build());
    if (chunkCount == 0) {
      firstResult = scanResult;
    } else {
      CachedScanResults.write(scannerId, chunkCount, scanResult);
    }
    chunkCount++;
    chunkSize = 0;
  }

  public boolean isSameSchema(final EntitySchema other) {
    return schema != null && this.schema.getEntityName().equals(other.getEntityName());
  }

  public void setSchema(final EntitySchema schema, final String[] fieldNames) throws IOException {
    sealResults();

    this.schema = schema;
    this.rows = new JsonEntityDataRows.Builder(schema, fieldNames);
  }

  private static final int MAX_CHUNK_SIZE = 1 << 20;
  public void add(final EntityDataRow row) throws IOException {
    final int rowSize = row.size();
    if (!isSameSchema(row.getSchema()) || (chunkSize > 0 && (chunkSize + rowSize) > MAX_CHUNK_SIZE)) {
      setSchema(row.getSchema(), null);
    }
    rows.add(row);
    chunkSize += rowSize;
    rowsSize += rowSize;
    rowCount++;
  }

  protected void incRowRead() {
    rowRead++;
  }

  public CachedScanResults newCachedScanResults() {
    return new CachedScanResults(scannerId, chunkCount);
  }

  public static class CachedScanResults {
    private final String scannerId;
    private final int count;
    private int index;

    private CachedScanResults(final String scannerId, final int count) {
      this.scannerId = scannerId;
      this.index = 0;
      this.count = count;
    }

    public String getScannerId() {
      return scannerId;
    }

    public boolean hasMore() {
      return index < count;
    }

    public ScanResult poll() throws IOException {
      if (index++ >= count) return null;

      final ScanResult result;
      final File file = FileUtil.tempFile("rs." + scannerId + "." + index);
      try (FileInputStream stream = new FileInputStream(file)) {
        try (GZIPInputStream gz = new GZIPInputStream(stream)) {
          result = CborFormat.INSTANCE.fromStream(gz, ScanResult.class);
        }
      }

      file.delete();
      return result;
    }

    public static void write(final String scannerId, final int index, final ScanResult result) throws IOException {
      final File file = FileUtil.tempFile("rs." + scannerId + "." + index);
      try (FileOutputStream stream = new FileOutputStream(file)) {
        try (GZIPOutputStream gz = new GZIPOutputStream(stream)) {
          CborFormat.INSTANCE.addToStream(gz, result);
        }
      }
    }
  }
}
