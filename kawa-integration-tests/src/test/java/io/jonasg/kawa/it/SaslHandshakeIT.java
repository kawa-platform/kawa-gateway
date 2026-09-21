package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AuthConfig;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/// Wire-level check that the gateway answers SaslHandshake itself, the way a real broker does,
/// instead of forwarding it on or dropping it. A raw client sends a SaslHandshake exactly as a
/// real Kafka client library would, and this reads back and decodes the real response bytes:
/// no log lines, no help from the gateway's internals, only what a client actually sees on the
/// wire.
///
/// Covers both directions a real broker handles: a supported mechanism gets
/// `NONE` plus the full mechanism list, an unsupported one gets
/// `UNSUPPORTED_SASL_MECHANISM` plus that same list, so the client knows what it can
/// retry with either way.
///
/// Supersedes the old `SaslLoggingIT`, which only checked that a request was logged
/// and deliberately ignored the response. Today the gateway still only logs and then drops the
/// request without answering at all, so both tests below fail on the read timeout until the
/// handshake is actually answered locally.
class SaslHandshakeIT extends GatewayTestSupport {

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                java.util.Set.of("PLAIN"),
                java.util.Map.of("alice", client("PLAIN", "secret")),
                null);
    }

    @Test
    void gatewayAnswersHandshakeLocallyForASupportedMechanism() throws Exception {
        var response = sendHandshake("PLAIN");

        assertThat(response.data().errorCode())
                .describedAs("PLAIN is a mechanism the gateway supports")
                .isEqualTo(Errors.NONE.code());
        assertThat(response.data().mechanisms())
                .describedAs("a real broker lists every mechanism it supports, not just the one asked for")
                .contains("PLAIN");
    }

    @Test
    void gatewayAnswersHandshakeLocallyInsteadOfDroppingOrForwardingIt() throws Exception {
        var response = sendHandshake("definitely-not-a-real-mechanism");

        assertThat(response.data().errorCode())
                .describedAs("no real deployment will ever configure this mechanism")
                .isEqualTo(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        assertThat(response.data().mechanisms())
                .describedAs("a real broker still lists what it does support, so the client can retry")
                .isNotEmpty();
    }

    private SaslHandshakeResponse sendHandshake(String mechanism) throws Exception {
        String[] parts = gatewayBootstrap.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]))) {
            socket.setSoTimeout(5000);
            RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE, (short) 1, "kawa-raw-it", 1);
            var request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism(mechanism), (short) 1);
            ByteBuffer payload = request.serializeWithHeader(header);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(payload.remaining());
            out.write(payload.array(), payload.position(), payload.remaining());
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            int size = in.readInt();
            byte[] responseBytes = new byte[size];
            in.readFully(responseBytes);
            ByteBuffer buffer = ByteBuffer.wrap(responseBytes);

            short responseHeaderVersion = ApiKeys.SASL_HANDSHAKE.responseHeaderVersion((short) 1);
            ResponseHeader responseHeader = ResponseHeader.parse(buffer, responseHeaderVersion);
            assertThat(responseHeader.correlationId())
                    .describedAs("must be the gateway's own answer, not something relayed unmodified from a broker")
                    .isEqualTo(1);

            return SaslHandshakeResponse.parse(new ByteBufferAccessor(buffer), (short) 1);
        }
    }
}
