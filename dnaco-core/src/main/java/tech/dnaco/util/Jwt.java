/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

import com.google.gson.JsonObject;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.compression.GzipUtil;
import tech.dnaco.compression.ZstdUtil;
import tech.dnaco.strings.StringUtil;

public class Jwt {
  public static final String CLAIM_AUDIENCE = "aud";
  public static final String CLAIM_EXPIRATION_TIME = "exp";
  public static final String CLAIM_ISSUER = "iss";
  public static final String CLAIM_ISSUED_AT = "iat";
  public static final String CLAIM_NOT_BEFORE = "nbf";
  public static final String CLAIM_SUBJECT = "sub";

  private final JsonObject dat;

  public Jwt(final String issuer, final long validity, final TimeUnit validityUnit) {
    this(issuer, System.currentTimeMillis(), validity, validityUnit);
  }

  public Jwt(final String issuer, final long issuedAtMs, final long validity, final TimeUnit validityUnit) {
    this.dat = new JsonObject();

    final long issuedAtSec = TimeUnit.MILLISECONDS.toSeconds(issuedAtMs);
    this.dat.addProperty(CLAIM_ISSUER, issuer);
    this.dat.addProperty(CLAIM_ISSUED_AT, issuedAtSec);
    this.dat.addProperty(CLAIM_EXPIRATION_TIME, issuedAtSec + validityUnit.toSeconds(validity));
  }

  private Jwt(final JsonObject jwtBody) {
    this.dat = jwtBody;
  }

  // ------------------------------------------------------------------------------------------
  // Get claim related
  // ------------------------------------------------------------------------------------------
  public String getIssuer() {
    return getClaimAsString(CLAIM_ISSUER);
  }

  public long getIssuedAt() {
    return TimeUnit.SECONDS.toMillis(getClaimAsLong(CLAIM_ISSUED_AT));
  }

  public long getExpirationTime() {
    return TimeUnit.SECONDS.toMillis(getClaimAsLong(CLAIM_EXPIRATION_TIME));
  }

  public long getClaimAsLong(final String key) {
    return dat.get(key).getAsLong();
  }

  public String getClaimAsString(final String key) {
    return JsonUtil.getString(dat, key, null);
  }

  public Set<String> getClaimAsStringSet(final String key) {
    final String[] values = getClaim(key, String[].class);
    return ArrayUtil.isEmpty(values) ? Collections.emptySet() : Set.of(values);
  }

  public <T> T getClaim(final String key, final Class<T> classOfT) {
    return JsonUtil.fromJson(dat.get(key), classOfT);
  }

  // ------------------------------------------------------------------------------------------
  // Set claim related
  // ------------------------------------------------------------------------------------------
  public Jwt addClaim(final String key, final boolean value) {
    dat.addProperty(key, value);
    return this;
  }

  public Jwt addClaim(final String key, final long value) {
    dat.addProperty(key, value);
    return this;
  }

  public Jwt addClaim(final String key, final String value) {
    dat.addProperty(key, value);
    return this;
  }

  public Jwt addClaim(final String key, final Object value) {
    dat.add(key, JsonUtil.toJsonTree(value));
    return this;
  }

  // ------------------------------------------------------------------------------------------
  // Sign related
  // ------------------------------------------------------------------------------------------
  public <TKey> String sign(final String kid, final String alg, final TKey key, final JwtSigner<TKey> signer)
      throws JwtException {
    final Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
    final String jwtHeader = JsonUtil.toJson(new JwtHeader(kid, alg));
    final String jwtBody = dat.toString();
    final String jwtToSign = b64.encodeToString(jwtHeader.getBytes()) + "." + b64.encodeToString(jwtBody.getBytes());
    final byte[] signature = signer.sign(kid, alg, key, jwtToSign.getBytes());
    return jwtToSign + "." + b64.encodeToString(signature);
  }

  // ------------------------------------------------------------------------------------------
  // Verify related
  // ------------------------------------------------------------------------------------------
  public static JwtParts decode(final String jwt) throws JwtException, IOException {
    final Base64.Decoder b64 = Base64.getUrlDecoder();

    // parse header
    final int headEof = jwt.indexOf('.');
    final byte[] rawJwtHead = b64.decode(jwt.substring(0, headEof));
    final JwtHeader jwtHeader = JsonUtil.fromJson(rawJwtHead, JwtHeader.class);

    // parse body
    final int bodyEof = jwt.indexOf('.', headEof + 1);
    final byte[] rawJwtBody = b64.decode(jwt.substring(headEof + 1, bodyEof));
    final JsonObject jwtBody = JsonUtil.fromJson(rawJwtBody, JsonObject.class);

    // parse signature
    final byte[] jwtSignature = b64.decode(jwt.substring(bodyEof + 1));

    // verify signature
    final String jwtSigned = jwt.substring(0, bodyEof);

    return new JwtParts(jwtHeader, new Jwt(jwtBody), jwtSigned.getBytes(), jwtSignature);
  }

  public static <TKey> Jwt verify(final String jwt, final TKey key, final JwtVerifier<TKey> verifier)
      throws JwtException, IOException {
    final JwtParts parts = decode(jwt);
    parts.verify(key, verifier);
    return parts.getBody();
  }

  @Override
  public String toString() {
    return "Jwt [claims=" + dat + "]";
  }

  // ------------------------------------------------------------------------------------------
  //  JWT callbaack related classes
  // ------------------------------------------------------------------------------------------
  public interface JwtVerifier<TKey> {
    void verifySignature(JwtHeader jwtHead, Jwt jwtBody, TKey key, byte[] jwtSigned, byte[] jwtSignature)
        throws JwtException;
  }

  public interface JwtSigner<TKey> {
    byte[] sign(String kid, String alg, TKey key, byte[] jwtToSign) throws JwtException;
  }

  public static final JwtVerifier<Void> JWT_NOOP_VERIFIER = new JwtVerifier<>() {
    @Override
    public void verifySignature(final JwtHeader jwtHead, final Jwt jwtBody, final Void key, final byte[] jwtSigned, final byte[] jwtSignature) {
      // no-op
    }
  };

  // ------------------------------------------------------------------------------------------
  //  JWT related classes
  // ------------------------------------------------------------------------------------------
  public static final class JwtHeader {
    private String typ;
    private String kid;
    private String alg;

    public JwtHeader(final String kid, final String alg) {
      this("JWT", kid, alg);
    }

    public JwtHeader(final String typ, final String kid, final String alg) {
      this.typ = typ;
      this.kid = kid;
      this.alg = alg;
    }

    public boolean isJwt() {
      return StringUtil.equals(typ, "JWT");
    }

    public String getKid() {
      return kid;
    }

    public void setKid(final String kid) {
      this.kid = kid;
    }

    public String getTyp() {
      return typ;
    }

    public void setTyp(final String typ) {
      this.typ = typ;
    }

    public String getAlg() {
      return alg;
    }

    public void setAlg(final String alg) {
      this.alg = alg;
    }

    @Override
    public String toString() {
      return "JwtHeader [alg=" + alg + ", kid=" + kid + ", typ=" + typ + "]";
    }
  }

  public static final class JwtParts {
    private final JwtHeader header;
    private final Jwt body;
    private final byte[] signed;
    private final byte[] signature;

    private JwtParts(final JwtHeader header, final Jwt body, final byte[] signed, final byte[] signature) {
      this.header = header;
      this.body = body;
      this.signed = signed;
      this.signature = signature;
    }

    public JwtHeader getHeader() {
      return header;
    }

    public Jwt getBody() {
      return body;
    }

    public String getKeyId() {
      return header.getKid();
    }

    public String getAlgorithm() {
      return header.getAlg();
    }

    public String getIssuer() {
      return body.getIssuer();
    }

    public long getExpirationTime() {
      return body.getExpirationTime();
    }

    public <TKey> void verify(final TKey key, final JwtVerifier<TKey> verifier) throws JwtException {
      verifier.verifySignature(header, body, key, signed, signature);
    }
  }

  // ------------------------------------------------------------------------------------------
  //  JWT exception classes
  // ------------------------------------------------------------------------------------------
  public static class JwtException extends Exception {
    private static final long serialVersionUID = 6207291634918997503L;

    public JwtException(final String message) {
      super(message);
    }

    public JwtException(final Throwable cause) {
      super(cause);
    }
  }

  public static class JwtInvalidSignatureException extends JwtException {
    private static final long serialVersionUID = 8032380170923558820L;

    public JwtInvalidSignatureException(final String message) {
      super(message);
    }

    public JwtInvalidSignatureException(final Throwable cause) {
      super(cause);
    }
  }

  public static void main(final String[] args) throws Exception {
    final String jwtString = "eyJ0eXAiOiJKV1QiLCJraWQiOiJxWnRrdHQxajhmbVBJc2o0RVBKWUFuYU9HSGxhbVlRalpQazF4UFdKV2ZJZWVZVVRiM1g2T1BGN1FGRTBFZ0lVRHZnT0w1Sk45V3FMcUJqcGV6c1hnIiwiYWxnIjoiRVMyNTYifQ.eyJpc3MiOiJnb2FsLmd1bGxpdmVybmV0LmNvbSIsImlhdCI6MTU5NTYxNjczOSwiZXhwIjoxNTk1NjIwMzM5LCJ0eXBlIjoiQUNDRVNTIiwicHJvamVjdElkIjoiZ29hbC5hZG1pbiIsInNjbHoiOiJjb20uZ3VsbGl2ZXJuZXQuc2VydmVyLmF1dGguVXNlclNlc3Npb24iLCJzY2x6cyI6WyJjb20uZ3VsbGl2ZXJuZXQuc2VydmVyLmF1dGguQXV0aEFjY2Vzc1Nlc3Npb24iXSwic2tleSI6ImZ2WkxBWkpxTUlXeEZieFhsVE43SmlFMlhWbUVIR24tdGZ2TGtwWWVqM0RHUXlTVlkxQTMzS0J4aFVHTWxYZ3JLTUgxR2paN2NnREk1RjUxdmNXN3lRIiwic2RhdCI6IkhnM21PR05rZ1g1LTludFhPaUZTcHJCNW82eHg4cm9RbVBDUzlHSDRCOXdIOTlpcHlNaklITTV4R1dvTG9qZEszZ0JqTEg5cHhwQ18tX3hEbzNjWS1HRUJjaXVaNU9XNXlYcTZxTzlDcTVLRXpEREZJV1lZSEE3QnRwcGs1cHgyWTZUQ2tvVmxlOW40TDBpWnhEUWF4Qk5tc2RmLW5JVEI1YXpHZ0xvemRwc0V2ZEZyVjBXVHd5NVUyVXo4NFA4b2J2LUIyam9VWGE3aXFNZS1zSHBLUktETjBqaUpCSjgxNUVOdTk0ZGdDTkFoSHVrYnpGMFRuYkZqaEUtaFJaX2k3dyIsImtleUFsZ28iOiJFQ19QMjU2IiwicHVibGljS2V5IjoiTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFV0k4RGcxcEVYMzFTaHdCQ2ZCQzF1M2ROZmxSOXZONW43NGt1djhmUXRsQWtUbHRVSEdqc200TWtPMmxfMkNmWW5lX3k3N0hNbHZNX282WXBHcWVPU2ciLCJwZXJtaXNzaW9ucyI6eyJ0YXNrIjpbIkNPREVfREVWRUxPUEVSIiwiTU9OSVRPUl9NQUlMQk9YX01BTkFHRU1FTlQiLCJTVUJTQ1JJUFRJT05fTUFOQUdFTUVOVCIsIlNDSEVEVUxFUl9UQVNLX01BTkFHRU1FTlQiLCJTQ0hFRFVMRVJfVEFTS19JTkZPIiwiRkxPV19ERVZFTE9QRVIiLCJGTE9XX0VYRUMiLCJNT05JVE9SX0ZUUF9NQU5BR0VNRU5UIiwiRVZFTlRfQlVTX01BTkFHRU1FTlQiLCJFWEVDVVRJT05fSElTVE9SWV9NQU5BR0VNRU5UIiwiU1VCU0NSSVBUSU9OX0lORk8iLCJNT05JVE9SX01BSUxCT1hfSU5GTyIsIk1PTklUT1JfRlRQX0lORk8iLCJFWEVDVVRJT05fSElTVE9SWV9JTkZPIiwiRlVOQ1RJT05fRVhFQyJdLCJrdHN5bmMtYWN0aXZhdGlvbiI6WyJBQ1RJVkFURSIsIkFDVElWQVRJT05fTUFOQUdFTUVOVCJdLCJkYXRhIjpbIlRBQkxFX0RFTEVURSIsIlRBQkxFX1NFTEVDVCIsIlRBQkxFX0lOU0VSVCIsIkRBVEFTT1VSQ0VfTUFOQUdFTUVOVCIsIlRBQkxFX1VQU0VSVCIsIlRBQkxFX1VQREFURSJdLCJhdXRoIjpbIlVTRVJfRURJVCIsIlVTRVJfQ0hBTkdFX1BBU1NXT1JEX1ZFUklGWSIsIlVTRVJfU0lHTl9VUCIsIlVTRVJfUkVTRVRfUEFTU1dPUkRfU0VORCIsIlBFUk1JU1NJT05fQVBJX0tFWV9JTkZPIiwiQVBJX0tFWV9DUkVBVEUiLCJQRVJNSVNTSU9OX0dST1VQX0lORk8iLCJVU0VSX1JFU0VUX1BBU1NXT1JEX1ZFUklGWSIsIkdST1VQX0lORk8iLCJBUElfS0VZX0RFTEVURSIsIlVTRVJfU0lHTl9JTiIsIkdST1VQX0xJU1QiLCJQRVJNSVNTSU9OX1VTRVJfSU5GTyIsIkdST1VQX0NSRUFURSIsIlVTRVJfQ1JFQVRFIiwiQVBJX0tFWV9JTkZPIiwiVVNFUl9BQ1RJVkFUSU9OX1NFTkQiLCJBUElfS0VZX0xJU1QiLCJHUk9VUF9FRElUIiwiR1JPVVBfREVMRVRFIiwiVVNFUl9ERUxFVEUiLCJERVZJQ0VfTUFOQUdFTUVOVCIsIlBFUk1JU1NJT05fVVNFUl9NQU5BR0VNRU5UIiwiR1JPVVBfQVBJX0tFWV9NQU5BR0VNRU5UIiwiQVBJX0tFWV9FRElUIiwiREVWSUNFX0RBVEFTT1VSQ0VfTUFOQUdFTUVOVCIsIkFVVEhfQ09ORklHIiwiUEVSTUlTU0lPTl9BVkFJTEFCTEUiLCJVU0VSX0lORk8iLCJVU0VSX0xJU1QiLCJHUk9VUF9VU0VSX01BTkFHRU1FTlQiLCJERVZJQ0VfREFUQV9NQU5BR0VNRU5UIiwiVVNFUl9BQ1RJVkFUSU9OX1ZFUklGWSIsIlBFUk1JU1NJT05fR1JPVVBfTUFOQUdFTUVOVCIsIlBFUk1JU1NJT05fQVBJX0tFWV9NQU5BR0VNRU5UIl0sInN1cGVyLWFkbWluIjpbIlBST0pFQ1RfQ1JFQVRFIiwiUFJPSkVDVF9FRElUIiwiRU5WSVJPTk1FTlRfTUFOQUdFTUVOVCJdLCJtZGMiOlsiREFUQVNPVVJDRV9NQU5BR0VNRU5UIiwiU0VORF9QVVNIIl0sIm1lc3NhZ2luZyI6WyJEQVRBU09VUkNFX01BTkFHRU1FTlQiLCJTRU5EX1NNUyIsIlNFTkRfQU5ZIiwiU0VORF9NQUlMIiwiU0VORF9QVVNIIl19LCJncm91cHMiOlsibmV3LWdyb3VwIiwiZ3JwNCIsImdycDIiXSwidWlkIjoiMSIsInVzZXJuYW1lIjoiYWRtaW4iLCJkZXZpY2VJZCI6InpYbmpnMGxSSGhZS0pGajB2WTdhLUMzbGZaMzNhS2R1NnpqNG1XRHdTWm8iLCJsYW5nIjoiZW4ifQ.MEYCIQChuFdRSxKTD_RE2p7tX12zzSoZ1Pq3DQt4VCqJFpS5sQIhAOO0TeX6zAPXvteknL4Nkl0EaN2Sf6PgTVgIvVjs0A28";
    System.out.println(jwtString.length());

    final String[] parts = jwtString.split(Pattern.quote("."));
    for (int i = 0; i < parts.length; ++i) {
      System.out.println(i + " -> " + parts[i].length());
      //System.out.println(i + " -> gz: " + Base64.getUrlEncoder().encodeToString(ZstdUtil.compress(Base64.getUrlDecoder().decode(parts[i]))));
    }

    final byte[] rawJwt = Base64.getUrlDecoder().decode(parts[1]);
    System.out.println("RAW: " + rawJwt.length + " b64: " + parts[1].length());
    final byte[] gzJwt = GzipUtil.compress(rawJwt);
    System.out.println(" -> gz: " + gzJwt.length + " b64: " + Base64.getUrlEncoder().encodeToString(gzJwt).length());
    final byte[] gz9Jwt = GzipUtil.compress(rawJwt, Deflater.BEST_COMPRESSION);
    System.out.println(" -> gz9: " + gz9Jwt.length + " b64: " + Base64.getUrlEncoder().encodeToString(gz9Jwt).length());

    final ByteArraySlice zstdJwt = ZstdUtil.compress(rawJwt, 0, rawJwt.length);
    final byte[] zstdRaw = Arrays.copyOfRange(zstdJwt.rawBuffer(), zstdJwt.offset(), zstdJwt.length());
    System.out.println(" -> zstd: " + zstdRaw.length + " b64: " + Base64.getUrlEncoder().encodeToString(zstdRaw).length());

    //if (true) return;

    final Jwt jwt = new Jwt("foo", 1, TimeUnit.HOURS);
    jwt.addClaim("project", "puppa");
    final String jwtEnc = jwt.sign("k123", "EC123", null, new JwtSigner<Void>(){
      @Override
      public byte[] sign(final String kid, final String alg, final Void key, final byte[] jwtToSign) {
        return new byte[] { 1, 2, 3 };
      }
    });
    Jwt.verify(jwtEnc, null, new JwtVerifier<String>(){
      @Override
      public void verifySignature(final JwtHeader jwtHead, final Jwt jwtBody, final String key, final byte[] jwtSigned, final byte[] jwtSignature)
          throws JwtException {
        System.out.println("VERIFY " + Arrays.toString(jwtSigned) + " -> " + Arrays.toString(jwtSignature));
      }
    });
    System.out.println(jwtEnc);
  }
}