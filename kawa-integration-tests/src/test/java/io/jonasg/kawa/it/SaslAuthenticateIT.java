package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AuthConfig;
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
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/// Wire-level check that the gateway answers SaslAuthenticate locally after a successful
/// handshake, instead of dropping the request.
class SaslAuthenticateIT extends GatewayTestSupport {

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                java.util.Set.of("PLAIN"),
                java.util.Map.of("alice", client("PLAIN", "secret")),
                null);
    }

    @Test
    void gatewayAuthenticatesKnownUserWithCorrectPassword() throws Exception {
        // given
        try (Socket socket = openSocket()) {
            sendHandshake(socket, "PLAIN", 1);

            // when
            var response = sendAuthenticate(socket, "\u0000alice\u0000secret", 2);

            // then
            assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
        }
    }

    @Test
    void gatewayRejectsWrongPasswordWithSaslAuthenticationFailed() throws Exception {
        // given
        try (Socket socket = openSocket()) {
            sendHandshake(socket, "PLAIN", 1);

            // when
            var response = sendAuthenticate(socket, "\u0000alice\u0000wrong", 2);

            // then
            assertThat(response.data().errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
            assertThat(response.data().errorMessage()).contains("Invalid username or password");
        }
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
            String plainPayload,
            int correlationId
    ) throws Exception {
        RequestHeader header = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, (short) 2, "kawa-raw-it", correlationId);
        var request = new SaslAuthenticateRequest(
                new SaslAuthenticateRequestData().setAuthBytes(plainPayload.getBytes(UTF_8)),
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
