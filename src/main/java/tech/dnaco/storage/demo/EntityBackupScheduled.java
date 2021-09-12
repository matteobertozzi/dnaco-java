package tech.dnaco.storage.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.security.MessageDigest;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.github.luben.zstd.ZstdOutputStream;
import com.gullivernet.commons.util.ScheduledTask;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.strings.HumansUtil;

public final class EntityBackupScheduled extends ScheduledTask {
  public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  @Override
  protected void execute() {
    final String[] projectIds = new File("STORAGE_DATA").list();
    for (int i = 0; i < projectIds.length; ++i) {
      Logger.setSession(LoggerSession.newSession(projectIds[i], Logger.getSession()));
      try {
        processProject(projectIds[i]);
      } catch (final Exception e) {
        Logger.error(e, "failed to process {}", projectIds[i]);
      } finally {
        Logger.stopSession();
      }
    }
  }

  private static void processProject(final String projectId) throws Exception {
    final long startTime = System.nanoTime();
    final ZonedDateTime now = ZonedDateTime.now();
    final File backupDir = new File(new File("backup"), DATE_FORMAT.format(now));
    backupDir.mkdirs();

    final File file = new File(backupDir, projectId);
    final LongValue size = new LongValue();
    final MessageDigest cksum = MessageDigest.getInstance("SHA3-512");
    try (ZstdOutputStream stream = new ZstdOutputStream(new FileOutputStream(file))) {
      Storage.getInstance(projectId).scanAll((key, rawVal) -> {
        final byte[] val = (rawVal != null) ? rawVal : BytesUtil.EMPTY_BYTES;
        IntEncoder.BIG_ENDIAN.writeFixed32(stream, key.length());
        IntEncoder.BIG_ENDIAN.writeFixed32(stream, val.length);
        size.add(key.length() + val.length);
        stream.write(key.buffer(), key.offset(), key.length());
        cksum.update(key.buffer(), key.offset(), key.length());
        stream.write(val);
        cksum.update(val);
      });
      stream.flush();
    }
    final String hash = BytesUtil.toHexString(cksum.digest());
    final File bkpFile = new File(backupDir, hash + "." + projectId);
    file.renameTo(bkpFile);
    Logger.info("backup {} completed in {} - data plain {} compressed {} - {}",
      projectId, HumansUtil.humanTimeSince(startTime),
      HumansUtil.humanSize(size.get()), HumansUtil.humanSize(bkpFile.length()),
      hash);
  }

  private static final class LongValue {
    private long value;

    public long get() {
      return value;
    }

    public void set(final long value) {
      this.value = value;
    }

    public long incrementAndGet() {
      return ++this.value;
    }

    public long add(final long delta) {
      this.value += delta;
      return this.value;
    }
  }
}
