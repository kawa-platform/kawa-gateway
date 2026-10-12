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
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.Tokens;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/// Issues kawa's admin tokens, ES256-signed JWTs carrying `iss`, `aud` (both the configured issuer),
/// `sub` (the admin user's stable subject), `client_id` (RFC 9068), `iat`, `exp` and `jti`. Session tokens
/// also carry `preferred_username`, the admin user's username at the time, for display. Every token names
/// the OAuth client it was issued to: [#STUDIO_CLIENT_ID] for admin users, [#OPERATOR_CLIENT_ID] for the
/// operator (in both `sub` and `client_id` when it acts as itself):
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

    /// The client id of kawa studio, the UI the admin users log in with.
    public static final String STUDIO_CLIENT_ID = "kawa-studio";

    /// The client id of the kawa operator, the automation that uses the `client_credentials` grant.
    public static final String OPERATOR_CLIENT_ID = "kawa-operator";

    /// The claim naming the OAuth client a token was issued to (RFC 9068).
    public static final String CLIENT_ID_CLAIM = "client_id";

    /// The claim naming who acts on the subject's behalf in a delegated token (RFC 8693 §4.1).
    public static final String ACT_CLAIM = "act";

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
        Instant expiry = now.plus(tokenTtl);
        return accessToken(claims(now, expiry).subject(subject).claim(CLIENT_ID_CLAIM, STUDIO_CLIENT_ID).build(), now, expiry);
    }

    /// A new access token for the operator, outside of any session and without a refresh token: the
    /// operator authenticates again when it expires (RFC 6749 §4.4.3).
    public BearerAccessToken issueForOperator() {
        Instant now = clock.instant();
        Instant expiry = now.plus(tokenTtl);
        return accessToken(claims(now, expiry).subject(OPERATOR_CLIENT_ID).claim(CLIENT_ID_CLAIM, OPERATOR_CLIENT_ID).build(), now, expiry);
    }

    /// A new access token for the operator acting as `principal` (token exchange, RFC 8693): `sub` is
    /// the principal, `act.sub` and `client_id` the operator.
    public BearerAccessToken issueDelegated(String principal) {
        Instant now = clock.instant();
        Instant expiry = now.plus(tokenTtl);
        JWTClaimsSet claims = claims(now, expiry)
                .subject(principal)
                .claim(CLIENT_ID_CLAIM, OPERATOR_CLIENT_ID)
                .claim(ACT_CLAIM, Map.of("sub", OPERATOR_CLIENT_ID))
                .build();
        String jwt = sign(ACCESS_TOKEN_TYPE, claims);
        return new BearerAccessToken(jwt, Duration.between(now, expiry).toSeconds(), null, TokenTypeURI.ACCESS_TOKEN);
    }

    /// Starts a session for `subject`, currently named `username` (after a login): an access token
    /// and a refresh token that is valid until the session ends.
    public Tokens newSession(String subject, String username) {
        return refreshSession(subject, username, clock.instant().plus(sessionTtl));
    }

    /// Continues the session of `subject` that ends at `sessionExpiry`: a new access token and a
    /// rotated refresh token with the same end, both naming the admin user's current `username`.
    public Tokens refreshSession(String subject, String username, Instant sessionExpiry) {
        Instant now = clock.instant();
        Instant accessExpiry = now.plus(tokenTtl).isBefore(sessionExpiry) ? now.plus(tokenTtl) : sessionExpiry;
        String refreshToken = sign(REFRESH_TOKEN_TYPE, claims(subject, username, now, sessionExpiry));
        return new Tokens(accessToken(claims(subject, username, now, accessExpiry), now, accessExpiry), new RefreshToken(refreshToken));
    }

    private BearerAccessToken accessToken(JWTClaimsSet claims, Instant now, Instant expiry) {
        String jwt = sign(ACCESS_TOKEN_TYPE, claims);
        return new BearerAccessToken(jwt, Duration.between(now, expiry).toSeconds(), null);
    }

    private JWTClaimsSet claims(String subject, String username, Instant now, Instant expiry) {
        return claims(now, expiry).subject(subject).claim(CLIENT_ID_CLAIM, STUDIO_CLIENT_ID).claim("preferred_username", username).build();
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
