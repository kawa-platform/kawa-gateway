package io.jonasg.kawa.http.auth.oauth;

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
///   redirect URI, PKCE challenge, state) from `/oauth/authorize` through the login UI to `/oauth/login`
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

    /// A validated authorization request waiting for the user to log in.
    record PendingAuthorization(String clientId, URI redirectUri, String codeChallenge, @Nullable String state) {
    }

    /// A redeemed authorization code: who logged in, for which client, redirect URI and PKCE challenge.
    record AuthorizedCode(String subject, String clientId, URI redirectUri, String codeChallenge) {
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
                Set.of(CLIENT_ID, REDIRECT_URI, CODE_CHALLENGE, "exp", "jti"));
        this.codes = new TokenVerifier(keys, issuerName, CODE_TYPE,
                Set.of("sub", CLIENT_ID, REDIRECT_URI, CODE_CHALLENGE, "exp", "jti"));
    }

    /// The login challenge to hand to the login UI for `pending`.
    String challenge(PendingAuthorization pending) {
        Instant now = clock.instant();
        return issuer.sign(LOGIN_CHALLENGE_TYPE, withRequest(issuer.claims(now, now.plus(CHALLENGE_TTL)), pending).build());
    }

    /// The authorization request behind `challenge`, unless it is invalid or expired.
    Optional<PendingAuthorization> pending(String challenge) {
        return challenges.verify(challenge).map(claims -> new PendingAuthorization(
                string(claims, CLIENT_ID), URI.create(string(claims, REDIRECT_URI)), string(claims, CODE_CHALLENGE),
                stringOrNull(claims, STATE)));
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
                        URI.create(string(claims, REDIRECT_URI)), string(claims, CODE_CHALLENGE)));
    }

    private static JWTClaimsSet.Builder withRequest(JWTClaimsSet.Builder claims, PendingAuthorization pending) {
        return claims.claim(CLIENT_ID, pending.clientId())
                .claim(REDIRECT_URI, pending.redirectUri().toString())
                .claim(CODE_CHALLENGE, pending.codeChallenge())
                .claim(STATE, pending.state());
    }

    private static String string(JWTClaimsSet claims, String name) {
        String value = stringOrNull(claims, name);
        if (value == null) {
            throw new IllegalStateException("verified token without " + name);
        }
        return value;
    }

    private static @Nullable String stringOrNull(JWTClaimsSet claims, String name) {
        try {
            return claims.getStringClaim(name);
        } catch (java.text.ParseException e) {
            return null;
        }
    }
}
