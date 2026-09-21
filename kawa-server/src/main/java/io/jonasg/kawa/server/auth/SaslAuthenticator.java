package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;

import io.jonasg.kawa.config.ClientConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/// Gateway-owned SASL bootstrap: decides whether the client's requested mechanism is one
/// kawa supports and answers `SaslHandshake` locally, exactly as a real broker would - an
/// error code plus the full list of supported mechanisms either way, so a client that asked
/// for the wrong one knows what it can retry with.
///
/// Deliberately not an interceptor. SASL is a connection bootstrap concern - once real auth
/// lands, everything on a connection must be gated until it authenticates, and the handshake
/// response is one the gateway originates itself, with no broker round trip to observe.
/// Neither fits the pipeline's per-api-key, request-then-broker-response shape, and nothing
/// in a `List<Interceptor>` guarantees this runs before every other interceptor for every
/// api key - the actual requirement for a gate. So this is called directly from
/// `KafkaClientRequestHandler`, the same place `ApiVersions` is already special-cased ahead
/// of the interceptor pipeline.
///
/// `SaslAuthenticate` auth bytes are validated by this class for PLAIN credentials.
///
/// The mechanisms and clients are an immutable snapshot replaced atomically via [reload]: a
/// reader on the hot path sees either the previous or the new auth state, never a
/// partially-applied one.
public final class SaslAuthenticator {

    /// Immutable snapshot of the dynamic auth state, published as a unit so a reload can never
    /// be observed half-applied.
    private record Snapshot(Set<String> mechanisms, Map<String, ClientConfig> clients) {
    }

    private volatile Snapshot snapshot;

    public SaslAuthenticator(Set<String> mechanisms) {
        this(mechanisms, Map.of());
    }

    public SaslAuthenticator(
            Set<String> mechanisms,
            Map<String, ClientConfig> clients
    ) {
        reload(mechanisms, clients);
    }

    /// Replaces the supported mechanisms and client directory with a new snapshot.
    ///
    /// Safe to call concurrently with readers: the new snapshot is assigned to a single
    /// `volatile` reference, so readers see either the previous or the new auth state, never a
    /// partially-applied one.
    public void reload(Set<String> mechanisms, Map<String, ClientConfig> clients) {
        this.snapshot = new Snapshot(Set.copyOf(mechanisms), Map.copyOf(clients));
    }

    public static boolean isSaslApi(int apiKey) {
        return apiKey == KafkaApiRegistry.SASL_HANDSHAKE || apiKey == KafkaApiRegistry.SASL_AUTHENTICATE;
    }

    /// Builds the handshake response: [Errors#NONE] when the requested mechanism is supported,
    /// [Errors#UNSUPPORTED_SASL_MECHANISM] otherwise - always listing every supported
    /// mechanism so the client knows what to retry with.
    public SaslHandshakeResponseData handleHandshake(SaslHandshakeRequestData request) {
        Set<String> mechanisms = snapshot.mechanisms();
        var response = new SaslHandshakeResponseData();
        response.setErrorCode(mechanisms.contains(request.mechanism())
                ? Errors.NONE.code()
                : Errors.UNSUPPORTED_SASL_MECHANISM.code());
        mechanisms.forEach(response.mechanisms()::add);
        return response;
    }

    /// Validates `SaslAuthenticate` auth bytes for PLAIN credentials.
    ///
    /// Expected payload is `authzid\0authcid\0password`; `authzid` may be empty.
    /// Unknown users, wrong passwords and malformed payloads all return the same generic
    /// failure to avoid user enumeration.
    public AuthenticationResult handleAuthenticate(SaslAuthenticateRequestData request) {
        var response = new SaslAuthenticateResponseData();

        var authBytes = request.authBytes();
        if (authBytes == null) {
            return authenticationFailed(response);
        }

        var parts = new String(authBytes, StandardCharsets.UTF_8).split("\\u0000", -1);
        if (parts.length != 3) {
            return authenticationFailed(response);
        }

        String username = parts[1];
        String password = parts[2];
        if (username.isEmpty()) {
            return authenticationFailed(response);
        }

        ClientConfig clientConfig = snapshot.clients().get(username);
        if (clientConfig == null || !clientConfig.password().verify(password)) {
            return authenticationFailed(response);
        }

        response.setErrorCode(Errors.NONE.code());
        return new AuthenticationResult.Success(username, response);
    }

    private static AuthenticationResult authenticationFailed(SaslAuthenticateResponseData response) {
        response.setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code());
        response.setErrorMessage("Invalid username or password");
        return new AuthenticationResult.Failure(response);
    }

}
