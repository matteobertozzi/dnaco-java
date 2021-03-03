package tech.dnaco.storage.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.arrays.IntArray;
import tech.dnaco.storage.blocks.BlockEntry;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;

public class StorageEntity {
  private final Storage storage;

  public StorageEntity(final Storage storage) {
    this.storage = storage;
  }

  public WriteResult insert(final String entity, final long txnId, final EntityData[] data) throws IOException {
    final WriteResult result = new WriteResult();
    for (int i = 0; i < data.length; ++i) {
      result.update(insert(entity, txnId, data[i]));
    }
    return result;
  }

  public WriteResult insert(final String entity, final long txnId, final EntityData data) throws IOException {
    final int rowCount = data.rowCount();

    final ArrayList<BlockEntry> entries = new ArrayList<>(rowCount * data.fields.length);
    final IntArray errors = new IntArray(rowCount);
    int inserted = 0;

    for (int i = 0; i < rowCount; ++i) {
      final int rowOffset = data.rowOffset(i);
      if (storage.getBlockManager().hasKey(data.key(entity, txnId, rowOffset))) {
        errors.add(i);
        continue;
      }

      inserted++;
      data.addBlockEntities(entries, entity, txnId, BlockEntry.FLAG_INSERT, rowOffset);
    }

    storage.addAll(entries);

    return new WriteResult().setInserted(inserted).setErrors(errors);
  }

  public WriteResult upsert(final String entity, final long txnId, final EntityData[] data) throws IOException {
    final WriteResult result = new WriteResult();
    for (int i = 0; i < data.length; ++i) {
      result.update(upsert(entity, txnId, data[i]));
    }
    return result;
  }

  public WriteResult upsert(final String entity, final long txnId, final EntityData data) throws IOException {
    final int rowCount = data.rowCount();

    final ArrayList<BlockEntry> entries = new ArrayList<>(rowCount * data.fields.length);
    final IntArray errors = new IntArray(rowCount);
    int inserted = 0;

    for (int i = 0; i < rowCount; ++i) {
      final int rowOffset = data.rowOffset(i);
      inserted++;
      data.addBlockEntities(entries, entity, txnId, BlockEntry.FLAG_UPSERT, rowOffset);
    }

    storage.addAll(entries);

    return new WriteResult().setInserted(inserted).setErrors(errors);
  }

  public WriteResult update(final String entity, final long txnId, final EntityData[] data) throws IOException {
    final WriteResult result = new WriteResult();
    for (int i = 0; i < data.length; ++i) {
      result.update(update(entity, txnId, data[i]));
    }
    return result;
  }

  public WriteResult update(final String entity, final long txnId, final EntityData data) throws IOException {
    final int rowCount = data.rowCount();

    final ArrayList<BlockEntry> entries = new ArrayList<>(rowCount * data.fields.length);
    final IntArray errors = new IntArray(rowCount);
    int updated = 0;

    for (int i = 0; i < rowCount; ++i) {
      final int rowOffset = data.rowOffset(i);
      updated++;
      data.addBlockEntities(entries, entity, txnId, BlockEntry.FLAG_UPDATE, rowOffset);
    }

    storage.addAll(entries);

    return new WriteResult().setUpdated(updated).setErrors(errors);
  }

  public static final class WriteResult {
    private int inserted;
    private int updated;
    private int errors;
    private int[] error_details;

    private WriteResult setInserted(final int value) {
      this.inserted = value;
      return this;
    }

    private WriteResult setUpdated(final int value) {
      this.updated = value;
      return this;
    }

    private WriteResult setErrors(final IntArray errors) {
      this.errors = errors.size();
      this.error_details = errors.drain();
      return this;
    }

    private void update(final WriteResult result) {
      this.inserted += result.inserted;
      this.updated += result.updated;
      this.errors += result.inserted;
      final int[] xErrors = new int[ArrayUtil.length(error_details) + ArrayUtil.length(result.error_details)];
      if (error_details != null) System.arraycopy(error_details, 0, xErrors, 0, error_details.length);
      if (result.error_details != null) System.arraycopy(result.error_details, 0, xErrors, ArrayUtil.length(error_details), error_details.length);
      this.error_details = xErrors;
    }
  }

  public static final class EntityData {
    private int keyCount;
    private String[] fields;
    private Object[] values;

    public int rowCount() {
      return values.length / (fields.length + keyCount);
    }

    public int rowOffset(final int rowIndex) {
      return rowIndex * (fields.length + keyCount);
    }

    public ByteArraySlice key(final String entity, final long txnId, final int rowOffset) {
      final RowKeyBuilder rowKey = new RowKeyBuilder();
      if (txnId > 0) {
        rowKey.add("_sys_txn_");
        rowKey.add(String.valueOf(txnId));
      }
      rowKey.add(entity);
      for (int i = 0; i < keyCount; ++i) {
        rowKey.add(String.valueOf(values[rowOffset + i]));
      }
      return new ByteArraySlice(rowKey.drain());
    }

    public ByteArraySlice key(final String entity, final long txnId, final int rowOffset, final int fieldIndex) {
      final RowKeyBuilder rowKey = new RowKeyBuilder();
      if (txnId > 0) {
        rowKey.add("_sys_txn_");
        rowKey.add(String.valueOf(txnId));
      }
      rowKey.add(entity);
      for (int i = 0; i < keyCount; ++i) {
        rowKey.add(String.valueOf(values[rowOffset + i]));
      }
      rowKey.add(fields[fieldIndex]);
      return new ByteArraySlice(rowKey.drain());
    }

    public ByteArraySlice value(final int rowOffset, final int fieldIndex) {
      final String v = String.valueOf(values[rowOffset + keyCount + fieldIndex]);
      return new ByteArraySlice(v.getBytes());
    }

    public void addBlockEntities(final List<BlockEntry> entries, final String entity, final long txnId,
        final long flags, final int rowOffset) {
      for (int k = 0; k < fields.length; ++k) {
        final BlockEntry entry = new BlockEntry();
        entry.setFlags(flags);
        entry.setKey(key(entity, txnId, rowOffset, k));
        entry.setValue(value(rowOffset, k));
        entries.add(entry);
      }
    }
  }
}
