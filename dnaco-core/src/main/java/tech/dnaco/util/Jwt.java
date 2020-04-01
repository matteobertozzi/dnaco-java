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
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;

import tech.dnaco.strings.StringUtil;

public class Jwt {
  public static final String CLAIM_AUDIENCE = "aud";
  public static final String CLAIM_EXPIRATION_TIME = "exp";
  public static final String CLAIM_ISSUER = "iss";
  public static final String CLAIM_ISSUED_AT = "iat";
  public static final String CLAIM_NOT_BEFORE = "nbf";
  public static final String CLAIM_SUBJECT = "sub";

  private final JsonObject dat;

  public Jwt(final String issuer, final long validity, TimeUnit validityUnit) {
    this(issuer, System.currentTimeMillis(), validity, validityUnit);
  }

  public Jwt(final String issuer, final long issuedAtMs, final long validity, TimeUnit validityUnit) {
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
    return dat.get(key).getAsString();
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

  public Jwt addClaim(final String key, Object value) {
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
    public void verifySignature(JwtHeader jwtHead, Jwt jwtBody, Void key, byte[] jwtSigned, byte[] jwtSignature) {
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

    public JwtHeader(String kid, String alg) {
      this("JWT", kid, alg);
    }

    public JwtHeader(String typ, String kid, String alg) {
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

    public void setKid(String kid) {
      this.kid = kid;
    }

    public String getTyp() {
      return typ;
    }

    public void setTyp(String typ) {
      this.typ = typ;
    }

    public String getAlg() {
      return alg;
    }

    public void setAlg(String alg) {
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

  public static void main(String[] args) throws Exception {
    Jwt jwt = new Jwt("foo", 1, TimeUnit.HOURS);
    jwt.addClaim("project", "puppa");
    String jwtEnc = jwt.sign("k123", "EC123", null, new JwtSigner<Void>(){
      @Override
      public byte[] sign(String kid, String alg, Void key, byte[] jwtToSign) {
        return new byte[] { 1, 2, 3 };
      }
    });
    System.out.println(jwtEnc);
  }
}