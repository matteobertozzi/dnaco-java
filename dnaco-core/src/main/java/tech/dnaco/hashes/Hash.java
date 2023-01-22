package tech.dnaco.hashes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import tech.dnaco.bytes.encoding.IntEncoder;

public abstract class Hash {
  public enum HashAlgo {
    MD5("MD5"),
    SHA_1("SHA-1"),
    SHA_224("SHA-224"),
    SHA_256("SHA-256"),
    SHA_384("SHA-384"),
    SHA_512("SHA-512"),
    SHA_512_224("SHA-512/224"),
    SHA_512_256("SHA-512/256"),
    SHA3_224("SHA3-224"),
    SHA3_256("SHA3-256"),
    SHA3_384("SHA3-384"),
    SHA3_512("SHA3-512");

    public final String algorithm;

    private HashAlgo(final String algorithm) {
      this.algorithm = algorithm;
    }

    public String algorithm() {
      return algorithm;
    }
  }

  public abstract Hash update(byte[] buf, int off, int len);
  public abstract void digestTo(byte[] buf, int off, int len);
  public abstract byte[] digest();
  public abstract int digestLength();
  public abstract int intValue();
  public abstract long longValue();

  public void digestTo(final byte[] buf) {
    digestTo(buf, 0, buf.length);
  }

  public Hash update(final byte[] buf) {
    return update(buf, 0, buf.length);
  }

  public Hash update(final String buf) {
    return update(buf.getBytes());
  }

  public Hash update(final byte b) {
    return update(new byte[] { b });
  }

  public Hash update(final int v) {
    final byte[] buf = new byte[4];
    IntEncoder.BIG_ENDIAN.writeFixed32(buf, 0, v);
    return update(buf, 0, 4);
  }

  public Hash update(final long v) {
    final byte[] buf = new byte[8];
    IntEncoder.BIG_ENDIAN.writeFixed32(buf, 0, v);
    return update(buf, 0, 8);
  }

  public Hash update(final UUID uuid) {
    update(uuid.getMostSignificantBits());
    update(uuid.getLeastSignificantBits());
    return this;
  }

  public Hash update(final Path file) throws IOException {
    try (InputStream stream = Files.newInputStream(file)) {
      update(stream);
    }
    return this;
  }

  public Hash update(final File file) throws IOException {
    try (FileInputStream stream = new FileInputStream(file)) {
      update(stream);
    }
    return this;
  }

  private Hash update(final InputStream stream) throws IOException {
    final byte[] buffer = new byte[8192];
    while (stream.available() > 0) {
      final int n = stream.read(buffer);
      if (n < 0) break;

      update(buffer, 0, n);
    }
    return this;
  }

  public static Hash of(final HashAlgo algorithm) {
    try {
      return new DigestHash(MessageDigest.getInstance(algorithm.algorithm()));
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Hash of(final MessageDigest digest) {
    return new DigestHash(digest);
  }

  private static final class DigestHash extends Hash {
    private final MessageDigest digest;

    private DigestHash(final MessageDigest digest) {
      this.digest = digest;
    }

    @Override
    public Hash update(final byte[] buf, final int off, final int len) {
      digest.update(buf, off, len);
      return this;
    }

    @Override
    public void digestTo(final byte[] buf, final int off, final int len) {
      final byte[] hash = digest();
      System.arraycopy(hash, 0, buf, off, Math.min(len, hash.length));
    }

    @Override
    public byte[] digest() {
      return digest.digest();
    }

    @Override
    public int digestLength() {
      return digest.getDigestLength();
    }

    @Override
    public int intValue() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long longValue() {
      throw new UnsupportedOperationException();
    }
  }
}
