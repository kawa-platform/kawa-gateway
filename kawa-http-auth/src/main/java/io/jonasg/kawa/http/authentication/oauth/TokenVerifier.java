package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import java.util.Optional;
import java.util.Set;

/// Verifies kawa-issued JWTs of one type (access tokens, refresh tokens, login challenges or
/// authorization codes): the ES256 signature against kawa's signing key, the `typ` header, `iss` equal to
/// the issuer, an accepted `aud` (the issuer unless other audiences are given), expiry, and the presence of
/// the required claims. A token of another type is rejected, so e.g. a refresh token can never be used as a
/// bearer token.
final class TokenVerifier {

    private final DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();

    /// A verifier of session tokens (access or refresh tokens) whose `aud` is one of `audiences`.
    TokenVerifier(SigningKeys keys, String issuer, Set<String> audiences, JOSEObjectType type) {
        this(keys, issuer, audiences, type, Set.of("sub", "iat", "exp", "jti"));
    }

    /// A verifier of tokens kawa issues to itself, whose `aud` is the issuer.
    TokenVerifier(SigningKeys keys, String issuer, JOSEObjectType type, Set<String> requiredClaims) {
        this(keys, issuer, Set.of(issuer), type, requiredClaims);
    }

    private TokenVerifier(SigningKeys keys, String issuer, Set<String> audiences, JOSEObjectType type, Set<String> requiredClaims) {
        processor.setJWSTypeVerifier(new DefaultJOSEObjectTypeVerifier<>(type));
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(
                JWSAlgorithm.ES256, new ImmutableJWKSet<>(keys.publicJwkSet())));
        processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                audiences,
                new JWTClaimsSet.Builder().issuer(issuer).build(),
                requiredClaims,
                null));
    }

    /// The token's claims when it is valid; empty when it is malformed, wrongly signed, of the wrong
    /// type, from another issuer or expired.
    Optional<JWTClaimsSet> verify(String token) {
        try {
            return Optional.of(processor.process(token, null));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
