package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The EC P-256 key kawa signs its admin access tokens with (ES256), plus the public half that
/// verifies them and is published as the JWKS.
///
/// The key comes from `admin.auth.settings.signingKey` (PEM). Without one a key is generated at startup:
/// fine for development, but tokens then stop working after a restart and are not accepted by
/// other gateway replicas.
public final class SigningKeys {

    private static final Logger log = LoggerFactory.getLogger(SigningKeys.class);

    private final ECKey signingKey;

    private SigningKeys(ECKey signingKey) {
        this.signingKey = signingKey;
    }

    /// Loads the key from PEM, or generates one when `pem` is `null`.
    public static SigningKeys from(@Nullable String pem) {
        if (pem == null) {
            log.warn("admin.auth.settings.signingKey is not set: generated a temporary signing key. Admin tokens will not "
                    + "survive a restart and are not accepted by other gateway replicas.");
            return generate();
        }
        return new SigningKeys(withThumbprintKeyId(EcPemKeyParser.parse(pem)));
    }

    /// A freshly generated key.
    public static SigningKeys generate() {
        try {
            return new SigningKeys(new ECKeyGenerator(Curve.P_256).keyIDFromThumbprint(true).generate());
        } catch (JOSEException e) {
            throw new IllegalStateException("could not generate an EC P-256 signing key", e);
        }
    }

    /// The private key used to sign tokens.
    public ECKey signingKey() {
        return signingKey;
    }

    /// The public key set used to verify tokens; safe to publish.
    public JWKSet publicJwkSet() {
        return new JWKSet(signingKey.toPublicJWK());
    }

    private static ECKey withThumbprintKeyId(ECKey key) {
        try {
            return new ECKey.Builder(key).keyIDFromThumbprint().build();
        } catch (JOSEException e) {
            throw new IllegalStateException("could not compute the signing key thumbprint", e);
        }
    }
}
