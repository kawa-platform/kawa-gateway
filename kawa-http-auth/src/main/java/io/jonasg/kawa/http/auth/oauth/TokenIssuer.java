package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

/// Issues kawa's admin tokens, ES256-signed JWTs carrying `iss`, `aud` (both the configured issuer),
/// `sub` (the admin username), `iat`, `exp` and `jti`:
///
/// - access tokens (`typ: at+jwt`, RFC 9068), valid for the token TTL
/// - refresh tokens (`typ: rt+jwt`), valid until the end of the session
///
/// A session starts at login and lasts the session TTL. Refreshing rotates both tokens but keeps
/// the session's end, and an access token never outlives its session.
public final class TokenIssuer {

    /// The JOSE `typ` of kawa access tokens.
    public static final JOSEObjectType ACCESS_TOKEN_TYPE = new JOSEObjectType("at+jwt");

    /// The JOSE `typ` of kawa refresh tokens.
    public static final JOSEObjectType REFRESH_TOKEN_TYPE = new JOSEObjectType("rt+jwt");

    private final SigningKeys keys;
    private final String issuer;
    private final Duration tokenTtl;
    private final Duration sessionTtl;
    private final Clock clock;
    private final ECDSASigner signer;

    public TokenIssuer(SigningKeys keys, String issuer, Duration tokenTtl, Duration sessionTtl, Clock clock) {
        this.keys = keys;
        this.issuer = issuer;
        this.tokenTtl = tokenTtl;
        this.sessionTtl = sessionTtl;
        this.clock = clock;
        try {
            this.signer = new ECDSASigner(keys.signingKey());
        } catch (JOSEException e) {
            throw new IllegalStateException("the signing key cannot sign ES256 tokens", e);
        }
    }

    /// A new access token for the admin user `subject`, outside of any session.
    public BearerAccessToken issue(String subject) {
        Instant now = clock.instant();
        return accessToken(subject, now, now.plus(tokenTtl));
    }

    /// Starts a session for `subject` (after a login): an access token and a refresh token that is
    /// valid until the session ends.
    public Tokens newSession(String subject) {
        return refreshSession(subject, clock.instant().plus(sessionTtl));
    }

    /// Continues the session of `subject` that ends at `sessionExpiry`: a new access token and a
    /// rotated refresh token with the same end.
    public Tokens refreshSession(String subject, Instant sessionExpiry) {
        Instant now = clock.instant();
        Instant accessExpiry = now.plus(tokenTtl).isBefore(sessionExpiry) ? now.plus(tokenTtl) : sessionExpiry;
        String refreshToken = sign(REFRESH_TOKEN_TYPE, claims(subject, now, sessionExpiry));
        return new Tokens(accessToken(subject, now, accessExpiry), new RefreshToken(refreshToken));
    }

    private BearerAccessToken accessToken(String subject, Instant now, Instant expiry) {
        String jwt = sign(ACCESS_TOKEN_TYPE, claims(subject, now, expiry));
        return new BearerAccessToken(jwt, Duration.between(now, expiry).toSeconds(), null);
    }

    private JWTClaimsSet claims(String subject, Instant now, Instant expiry) {
        return claims(now, expiry).subject(subject).build();
    }

    /// The claims every kawa token carries (`iss`, `aud`, `iat`, `exp`, `jti`), to be completed by the caller.
    JWTClaimsSet.Builder claims(Instant now, Instant expiry) {
        return new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(issuer)
                .issueTime(Date.from(now))
                .expirationTime(Date.from(expiry))
                .jwtID(UUID.randomUUID().toString());
    }

    /// Signs `claims` as a JWT of `type` with kawa's signing key.
    String sign(JOSEObjectType type, JWTClaimsSet claims) {
        var header = new JWSHeader.Builder(JWSAlgorithm.ES256)
                .type(type)
                .keyID(keys.signingKey().getKeyID())
                .build();
        var jwt = new SignedJWT(header, claims);
        try {
            jwt.sign(signer);
        } catch (JOSEException e) {
            throw new IllegalStateException("could not sign the " + type + " token", e);
        }
        return jwt.serialize();
    }
}
