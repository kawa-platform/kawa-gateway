package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jwt.JWTClaimsSet;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/// The state of the authorization code flow between `/oauth/authorize`, the external login UI and the
/// token endpoint, kept as short-lived signed JWTs so that every gateway replica can continue a flow
/// another replica started:
///
/// - a **login challenge** (`lc+jwt`, 5 minutes) carries the validated authorization request (client,
///   redirect URI, PKCE challenge, state, and the audience of the tokens to issue) from `/oauth/authorize`
///   through the login UI to `/oauth/login`
/// - an **authorization code** (`ac+jwt`, 60 seconds) carries the same plus the logged-in user to the
///   token endpoint
///
/// Codes are single-use: each replica remembers the codes it has redeemed until they expire. A code is
/// useless without the PKCE verifier that only the client holds.
final class AuthorizationFlow {

    static final JOSEObjectType LOGIN_CHALLENGE_TYPE = new JOSEObjectType("lc+jwt");
    static final JOSEObjectType CODE_TYPE = new JOSEObjectType("ac+jwt");
    static final Duration CHALLENGE_TTL = Duration.ofMinutes(5);
    static final Duration CODE_TTL = Duration.ofSeconds(60);

    private static final String CLIENT_ID = "client_id";
    private static final String REDIRECT_URI = "redirect_uri";
    private static final String CODE_CHALLENGE = "code_challenge";
    private static final String STATE = "state";
    private static final String TOKEN_AUDIENCE = "token_aud";
    private static final String CLIENT_NAME = "client_name";
    private static final String REGISTERED = "registered";

    /// A validated authorization request waiting for the user to log in; `audience` is the `aud` of the tokens
    /// the client gets. `clientName` and `registered` are what the login UI shows the user about who is asking: the
    /// client's display name and whether an admin registered it.
    record PendingAuthorization(
            String clientId,
            String clientName,
            boolean registered,
            URI redirectUri,
            String codeChallenge,
            @Nullable String state,
            String audience
    ) {
    }

    /// A redeemed authorization code: who logged in, for which client, redirect URI, PKCE challenge and token
    /// audience.
    record AuthorizedCode(
            String subject,
            String clientId,
            URI redirectUri,
            String codeChallenge,
            String audience
    ) {
    }

    private final TokenIssuer issuer;
    private final TokenVerifier challenges;
    private final TokenVerifier codes;
    private final Clock clock;
    private final Map<String, Instant> redeemed = new ConcurrentHashMap<>();

    AuthorizationFlow(SigningKeys keys, String issuerName, TokenIssuer issuer, Clock clock) {
        this.issuer = issuer;
        this.clock = clock;
        this.challenges = new TokenVerifier(keys, issuerName, LOGIN_CHALLENGE_TYPE,
                Set.of(CLIENT_ID, REDIRECT_URI, CODE_CHALLENGE, TOKEN_AUDIENCE, "exp", "jti"));
        this.codes = new TokenVerifier(keys, issuerName, CODE_TYPE,
                Set.of("sub", CLIENT_ID, REDIRECT_URI, CODE_CHALLENGE, TOKEN_AUDIENCE, "exp", "jti"));
    }

    /// The login challenge to hand to the login UI for `pending`.
    String challenge(PendingAuthorization pending) {
        Instant now = clock.instant();
        return issuer.sign(LOGIN_CHALLENGE_TYPE, withRequest(issuer.claims(now, now.plus(CHALLENGE_TTL)), pending).build());
    }

    /// The authorization request behind `challenge`, unless it is invalid or expired.
    Optional<PendingAuthorization> pending(String challenge) {
        return challenges.verify(challenge).map(claims -> new PendingAuthorization(
                string(claims, CLIENT_ID), nameOrId(claims), !Boolean.FALSE.equals(booleanOrNull(claims, REGISTERED)),
                URI.create(string(claims, REDIRECT_URI)), string(claims, CODE_CHALLENGE),
                stringOrNull(claims, STATE), string(claims, TOKEN_AUDIENCE)));
    }

    /// A single-use authorization code for `subject`, who completed the login for `pending`.
    String code(String subject, PendingAuthorization pending) {
        Instant now = clock.instant();
        return issuer.sign(CODE_TYPE, withRequest(issuer.claims(now, now.plus(CODE_TTL)), pending).subject(subject).build());
    }

    /// Redeems `code`: its content, unless it is invalid, expired or already redeemed on this replica.
    Optional<AuthorizedCode> redeem(String code) {
        Instant now = clock.instant();
        redeemed.values().removeIf(expiry -> expiry.isBefore(now));
        return codes.verify(code)
                .filter(claims -> redeemed.putIfAbsent(claims.getJWTID(), claims.getExpirationTime().toInstant()) == null)
                .map(claims -> new AuthorizedCode(claims.getSubject(), string(claims, CLIENT_ID),
                        URI.create(string(claims, REDIRECT_URI)), string(claims, CODE_CHALLENGE), string(claims, TOKEN_AUDIENCE)));
    }

    private static JWTClaimsSet.Builder withRequest(JWTClaimsSet.Builder claims, PendingAuthorization pending) {
        return claims.claim(CLIENT_ID, pending.clientId())
                .claim(REDIRECT_URI, pending.redirectUri().toString())
                .claim(CODE_CHALLENGE, pending.codeChallenge())
                .claim(CLIENT_NAME, pending.clientName())
                .claim(REGISTERED, pending.registered())
                .claim(STATE, pending.state())
                .claim(TOKEN_AUDIENCE, pending.audience());
    }

    private static String string(JWTClaimsSet claims, String name) {
        String value = stringOrNull(claims, name);
        if (value == null) {
            throw new IllegalStateException("verified token without " + name);
        }
        return value;
    }

    /// The client's display name, or its id for a challenge issued before challenges carried one.
    private static String nameOrId(JWTClaimsSet claims) {
        String name = stringOrNull(claims, CLIENT_NAME);
        return name != null ? name : string(claims, CLIENT_ID);
    }

    private static @Nullable Boolean booleanOrNull(JWTClaimsSet claims, String name) {
        try {
            return claims.getBooleanClaim(name);
        } catch (java.text.ParseException e) {
            return null;
        }
    }

    private static @Nullable String stringOrNull(JWTClaimsSet claims, String name) {
        try {
            return claims.getStringClaim(name);
        } catch (java.text.ParseException e) {
            return null;
        }
    }
}
