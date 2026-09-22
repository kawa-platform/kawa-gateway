package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit-level check of real SASL handshake handling. [SaslAuthenticator] decides locally
/// whether the client's requested mechanism is one kawa's own user directory supports, and
/// responds exactly as a real broker would: an `errorCode` plus the full list of
/// supported mechanisms either way, so a client that asked for the wrong one knows what it can
/// retry with. This is real protocol behaviour, not a log line to assert on.
///
/// Supersedes the old `SaslRequestLoggerTest`, which only checked that a request was
/// logged. The handshake and the PLAIN `SaslAuthenticate` credential check are covered here;
/// SCRAM-SHA-256/512 exchanges are covered once the SCRAM flow lands.
class SaslAuthenticatorTest {

    private static ClientConfig client(String mechanism, String password) {
        return new ClientConfig(mechanism,
                HashedPassword.fromPlaintext(Mechanism.fromWireName(mechanism), password));
    }

    private static ClientSession session() {
        return new ClientSession(new EmbeddedChannel());
    }

    private static SaslHandshakeRequestData handshake(String mechanism) {
        return new SaslHandshakeRequestData().setMechanism(mechanism);
    }

    private static SaslAuthenticateRequestData plainAuthenticate(String username, String password) {
        return new SaslAuthenticateRequestData()
                .setAuthBytes(("\u0000" + username + "\u0000" + password).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void isSaslApiRecognisesOnlyTheTwoSaslApis() {
        assertThat(SaslAuthenticator.isSaslApi(KafkaApiRegistry.SASL_HANDSHAKE)).isTrue();
        assertThat(SaslAuthenticator.isSaslApi(KafkaApiRegistry.SASL_AUTHENTICATE)).isTrue();
        assertThat(SaslAuthenticator.isSaslApi(KafkaApiRegistry.METADATA)).isFalse();
        assertThat(SaslAuthenticator.isSaslApi(KafkaApiRegistry.API_VERSIONS)).isFalse();
    }

    @Test
    void respondsWithNoErrorAndTheFullMechanismListForASupportedMechanism() {
        var authenticator = new SaslAuthenticator();

        var response = authenticator.handleHandshake(session(), handshake("PLAIN"));

        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");
    }

    @Test
    void respondsWithUnsupportedMechanismErrorButStillListsWhatIsSupported() {
        var authenticator = new SaslAuthenticator();

        var response = authenticator.handleHandshake(session(), handshake("GSSAPI"));

        assertThat(response.errorCode()).isEqualTo(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        assertThat(response.mechanisms())
                .describedAs("a real broker still returns what it does support, so the client can retry")
                .containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");
    }

    @Test
    void authenticatesPlainWithKnownClientAndCorrectPassword() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));
        var session = session();
        authenticator.handleHandshake(session, handshake("PLAIN"));

        // when
        var result = authenticator.handleAuthenticate(session, plainAuthenticate("alice", "secret"));

        // then
        assertThat(result).isInstanceOf(AuthenticationResult.Success.class);
        var success = (AuthenticationResult.Success) result;
        assertThat(success.username()).isEqualTo("alice");
        assertThat(success.response().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(success.response().errorMessage()).isNullOrEmpty();
    }

    @Test
    void rejectsUnknownClientWithoutLeakingWhetherTheUsernameExists() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));
        var session = session();
        authenticator.handleHandshake(session, handshake("PLAIN"));

        // when
        var result = authenticator.handleAuthenticate(session, plainAuthenticate("bob", "secret"));

        // then
        assertThat(result).isInstanceOf(AuthenticationResult.Failure.class);
        var failure = (AuthenticationResult.Failure) result;
        assertThat(failure.response().errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
        assertThat(failure.response().errorMessage()).contains("Invalid username or password");
    }

    @Test
    void rejectsMalformedPlainPayload() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));
        var session = session();
        authenticator.handleHandshake(session, handshake("PLAIN"));

        // when
        var result = authenticator.handleAuthenticate(session, new SaslAuthenticateRequestData()
                .setAuthBytes("not-a-plain-payload".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(result).isInstanceOf(AuthenticationResult.Failure.class);
        var failure = (AuthenticationResult.Failure) result;
        assertThat(failure.response().errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
        assertThat(failure.response().errorMessage()).contains("Invalid username or password");
    }

    @Test
    void authenticateWithoutHandshakeReturnsIllegalSaslState() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));

        // when
        var result = authenticator.handleAuthenticate(session(), plainAuthenticate("alice", "secret"));

        // then
        assertThat(result).isInstanceOf(AuthenticationResult.Failure.class);
        var failure = (AuthenticationResult.Failure) result;
        assertThat(failure.response().errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());
    }

    @Test
    void sessionClosedDropsPerSessionState() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));
        var session = session();
        authenticator.handleHandshake(session, handshake("PLAIN"));

        // when
        authenticator.sessionClosed(session);
        var result = authenticator.handleAuthenticate(session, plainAuthenticate("alice", "secret"));

        // then
        assertThat(result).isInstanceOf(AuthenticationResult.Failure.class);
        var failure = (AuthenticationResult.Failure) result;
        assertThat(failure.response().errorCode()).isEqualTo(Errors.ILLEGAL_SASL_STATE.code());

        // and a different session is unaffected
        var other = session();
        authenticator.handleHandshake(other, handshake("PLAIN"));
        assertThat(authenticator.handleAuthenticate(other, plainAuthenticate("alice", "secret")))
                .isInstanceOf(AuthenticationResult.Success.class);
    }

    @Test
    void reloadReplacesMechanismsAndClients() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));

        // when
        authenticator.reload(Set.of("PLAIN"), Map.of("bob", client("PLAIN", "hunter2")));

        // then
        var session = session();
        var handshake = authenticator.handleHandshake(session, handshake("PLAIN"));
        assertThat(handshake.errorCode()).isEqualTo(Errors.NONE.code());

        var oldUser = authenticator.handleAuthenticate(session, plainAuthenticate("alice", "secret"));
        assertThat(oldUser).isInstanceOf(AuthenticationResult.Failure.class);

        authenticator.handleHandshake(session, handshake("PLAIN"));
        var newUser = authenticator.handleAuthenticate(session, plainAuthenticate("bob", "hunter2"));
        assertThat(newUser).isInstanceOf(AuthenticationResult.Success.class);
    }

    @Test
    void reloadWithEmptyStateRejectsEverything() {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));

        // when
        authenticator.reload(Set.of(), Map.of());

        // then
        var session = session();
        var handshake = authenticator.handleHandshake(session, handshake("PLAIN"));
        assertThat(handshake.errorCode()).isEqualTo(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        var authenticate = authenticator.handleAuthenticate(session, plainAuthenticate("alice", "secret"));
        assertThat(authenticate).isInstanceOf(AuthenticationResult.Failure.class);
    }

    @Test
    void reloadIsSafeDuringConcurrentReads() throws Exception {
        // given
        var authenticator = new SaslAuthenticator(Set.of("PLAIN"), Map.of("alice", client("PLAIN", "secret")));
        var first = Map.of("alice", client("PLAIN", "secret"));
        var second = Map.of("bob", client("PLAIN", "hunter2"));
        var failure = new AtomicReference<Throwable>();

        // when
        var writer = new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                authenticator.reload(Set.of("PLAIN"), i % 2 == 0 ? first : second);
            }
        });
        var reader = new Thread(() -> {
            try {
                for (int i = 0; i < 50; i++) {
                    var session = session();
                    authenticator.handleHandshake(session, handshake("PLAIN"));
                    authenticator.handleAuthenticate(session, plainAuthenticate("alice", "secret"));
                    authenticator.handleAuthenticate(session, plainAuthenticate("bob", "hunter2"));
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        writer.start();
        reader.start();
        writer.join();
        reader.join();

        // then
        assertThat(failure).hasValue(null);
    }
}
