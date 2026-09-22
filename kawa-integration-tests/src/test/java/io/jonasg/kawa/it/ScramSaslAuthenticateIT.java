package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslClient;
import org.junit.jupiter.api.Test;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/// Wire-level check that the gateway runs the real SCRAM challenge-response exchange over a
/// raw socket: a client-side `ScramSaslClient` drives the exchange through `SaslAuthenticate`
/// frames and completes with a verified server signature.
class ScramSaslAuthenticateIT extends GatewayTestSupport {

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                Set.of("SCRAM-SHA-256"),
                Map.of("alice", client("SCRAM-SHA-256", "secret")),
                null);
    }

    @Override
    protected RbacConfig rbacConfig() {
        var role = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        return new RbacConfig(
                Map.of("allow-all", role),
                Map.of("scram-it", new GroupConfig(List.of("alice"), List.of("allow-all"))));
    }

    @Test
    void scramExchangeOverRawSocket() throws Exception {
        // given
        try (Socket socket = openSocket()) {
            var handshake = sendHandshake(socket, "SCRAM-SHA-256", 1);
            assertThat(handshake.data().errorCode()).isEqualTo(Errors.NONE.code());

            var client = scramClient("alice", "secret");

            // when — client-first, expect server-first challenge
            var first = sendAuthenticate(socket, client.evaluateChallenge(new byte[0]), 2);
            assertThat(first.data().errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(first.data().authBytes()).isNotEmpty();
            byte[] clientFinal = client.evaluateChallenge(first.data().authBytes());

            // when — client-final, expect server-final signature
            var second = sendAuthenticate(socket, clientFinal, 3);
            assertThat(second.data().errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(second.data().authBytes()).isNotEmpty();

            // then — the client verifies the server signature and completes
            client.evaluateChallenge(second.data().authBytes());
            assertThat(client.isComplete()).isTrue();
        }
    }

    @Test
    void scramWrongPasswordOverRawSocket() throws Exception {
        // given
        try (Socket socket = openSocket()) {
            sendHandshake(socket, "SCRAM-SHA-256", 1);

            var client = scramClient("alice", "wrong");

            // when — client-first, expect server-first challenge
            var first = sendAuthenticate(socket, client.evaluateChallenge(new byte[0]), 2);
            assertThat(first.data().errorCode()).isEqualTo(Errors.NONE.code());
            byte[] clientFinal = client.evaluateChallenge(first.data().authBytes());

            // when — client-final with wrong password
            var second = sendAuthenticate(socket, clientFinal, 3);

            // then
            assertThat(second.data().errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
            assertThat(second.data().errorMessage()).contains("Invalid username or password");
        }
    }

    private static ScramSaslClient scramClient(String username, String password) throws Exception {
        return new ScramSaslClient(ScramMechanism.SCRAM_SHA_256, callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback nameCallback) {
                    nameCallback.setName(username);
                } else if (callback instanceof PasswordCallback passwordCallback) {
                    passwordCallback.setPassword(password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        });
    }

    private Socket openSocket() throws Exception {
        String[] parts = gatewayBootstrap.split(":");
        var socket = new Socket(parts[0], Integer.parseInt(parts[1]));
        socket.setSoTimeout(5000);
        return socket;
    }

    private SaslHandshakeResponse sendHandshake(
            Socket socket,
            String mechanism,
            int correlationId
    ) throws Exception {
        RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE, (short) 1, "kawa-raw-it", correlationId);
        var request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism(mechanism), (short) 1);
        writeRequest(socket, request.serializeWithHeader(header));

        ByteBuffer frame = readResponseFrame(socket);
        short responseHeaderVersion = ApiKeys.SASL_HANDSHAKE.responseHeaderVersion((short) 1);
        ResponseHeader responseHeader = ResponseHeader.parse(frame, responseHeaderVersion);
        assertThat(responseHeader.correlationId()).isEqualTo(correlationId);
        return SaslHandshakeResponse.parse(new ByteBufferAccessor(frame), (short) 1);
    }

    private SaslAuthenticateResponse sendAuthenticate(
            Socket socket,
            byte[] authBytes,
            int correlationId
    ) throws Exception {
        RequestHeader header = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, (short) 2, "kawa-raw-it", correlationId);
        var request = new SaslAuthenticateRequest(
                new SaslAuthenticateRequestData().setAuthBytes(authBytes),
                (short) 2);
        writeRequest(socket, request.serializeWithHeader(header));

        ByteBuffer frame = readResponseFrame(socket);
        short responseHeaderVersion = ApiKeys.SASL_AUTHENTICATE.responseHeaderVersion((short) 2);
        ResponseHeader responseHeader = ResponseHeader.parse(frame, responseHeaderVersion);
        assertThat(responseHeader.correlationId()).isEqualTo(correlationId);
        return SaslAuthenticateResponse.parse(new ByteBufferAccessor(frame), (short) 2);
    }

    private static void writeRequest(
            Socket socket,
            ByteBuffer payload
    ) throws Exception {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(payload.remaining());
        out.write(payload.array(), payload.position(), payload.remaining());
        out.flush();
    }

    private static ByteBuffer readResponseFrame(Socket socket) throws Exception {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int size = in.readInt();
        byte[] responseBytes = new byte[size];
        in.readFully(responseBytes);
        return ByteBuffer.wrap(responseBytes);
    }
}
