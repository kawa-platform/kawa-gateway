package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.msk.auth.iam.internals.AWS4SignedPayloadGenerator;
import software.amazon.msk.auth.iam.internals.AuthenticationRequestParams;
import software.amazon.msk.auth.iam.internals.IAMSaslClient;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.security.Provider;
import java.security.Security;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/// AWS IAM sign-in (SASL `AWS_MSK_IAM`) for the gateway's broker connections.
///
/// The `aws-msk-iam-auth` library builds the signed sign-in payload, but it signs for the region
/// named in the broker hostname and uses its own region only as a fallback. Here the configured
/// `region` takes precedence, and the hostname's region is kept when none is configured. The Netty
/// broker connections ([BrokerMskIamSaslAuthenticator]) and the gateway's own Kafka clients (through
/// the JCA provider [#install] registers) sign the same way.
public final class AwsMskIam {

    private static final String PROVIDER_NAME = "kawa-aws-msk-iam";
    private static final String ACTION = "kafka-cluster:Connect";
    private static final String PROTOCOL = "https";
    private static final int EXPIRY_MINUTES = 15;

    /// The region the Kafka clients sign for, or `null` to derive it from each broker hostname.
    /// The gateway talks to one cluster, so one value serves every client in the process.
    private static volatile @Nullable Region signingRegion;

    private AwsMskIam() {
    }

    /// The region configured in `auth.brokerAuth.region`, or `null` when it is derived per host.
    public static @Nullable Region region(BrokerAuthConfig auth) {
        return auth.region() == null ? null : Region.of(auth.region());
    }

    /// Registers the JCA provider the gateway's Kafka clients resolve `AWS_MSK_IAM` through, signing
    /// for `auth`'s region. Call before a Kafka client connects; a later call replaces the earlier one.
    public static void install(BrokerAuthConfig auth) {
        signingRegion = region(auth);
        Security.removeProvider(PROVIDER_NAME);
        Security.insertProviderAt(new SigningProvider(), 1);
    }

    /// Presigns the sign-in request for `region`, or for the hostname's region when `region` is `null`.
    /// The request shape and expiry are the library's; only the signing region differs.
    static final class RegionSigner extends AWS4SignedPayloadGenerator {

        private final @Nullable Region region;

        RegionSigner(@Nullable Region region) {
            this.region = region;
        }

        @Override
        public SdkHttpFullRequest presignRequest(AuthenticationRequestParams params) {
            SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                    .method(SdkHttpMethod.GET)
                    .protocol(PROTOCOL)
                    .host(params.getHost())
                    .appendRawQueryParameter("Action", ACTION)
                    .build();
            Aws4PresignerParams signing = Aws4PresignerParams.builder()
                    .awsCredentials(params.getAwsCredentials())
                    .expirationTime(Instant.now().plus(EXPIRY_MINUTES, ChronoUnit.MINUTES))
                    .signingRegion(region == null ? params.getRegion() : region)
                    .signingName(params.getServiceScope())
                    .build();
            return Aws4Signer.create().presign(request, signing);
        }
    }

    /// The JCA provider that makes `AWS_MSK_IAM` resolve to [SigningSaslClientFactory].
    private static final class SigningProvider extends Provider {

        SigningProvider() {
            super(PROVIDER_NAME, "1.0", "SASL/AWS_MSK_IAM signing for the gateway's Kafka clients");
            put("SaslClientFactory." + BrokerAuthConfig.AWS_MSK_IAM, SigningSaslClientFactory.class.getName());
        }
    }

    /// Creates the library's SASL client with the gateway's signer. Public with a no-argument
    /// constructor, as the JCA requires for providers.
    public static final class SigningSaslClientFactory implements SaslClientFactory {

        public SigningSaslClientFactory() {
        }

        @Override
        public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
                                           String serverName, Map<String, ?> props, CallbackHandler callbacks)
                throws SaslException {
            for (String mechanism : mechanisms) {
                if (BrokerAuthConfig.AWS_MSK_IAM.equals(mechanism)) {
                    return new IAMSaslClient(mechanism, callbacks, serverName, new RegionSigner(signingRegion));
                }
            }
            return null;
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return new String[] {BrokerAuthConfig.AWS_MSK_IAM};
        }
    }
}
