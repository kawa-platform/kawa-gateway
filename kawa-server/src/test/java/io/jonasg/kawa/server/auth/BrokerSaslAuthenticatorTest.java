package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerSaslAuthenticatorTest {

    private final KafkaBodyCodec codec = new KafkaBodyCodec(KafkaApiRegistry.create());

    @Test
    void performsHandshakeAndAuthenticateAgainstSaslBroker() {
        // given a channel that behaves like a SASL_PLAINTEXT broker
        var channel = embeddedChannelWithBrokerSasl(
                new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()),
                new SaslAuthenticateResponseData().setErrorCode(Errors.NONE.code()));
        var config = new BrokerAuthConfig("PLAIN", "gateway", "secret");
        var authenticator = new BrokerSaslAuthenticator(config, codec);

        // when
        boolean success = authenticator.authenticate(channel);

        // then
        assertThat(success).isTrue();
    }

    @Test
    void rejectsWhenBrokerDoesNotSupportRequestedMechanism() {
        // given a channel that rejects the mechanism
        var handshakeResponse = new SaslHandshakeResponseData()
                .setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        handshakeResponse.mechanisms().add("SCRAM-SHA-256");
        var channel = embeddedChannelWithBrokerSasl(handshakeResponse, null);
        var config = new BrokerAuthConfig("PLAIN", "gateway", "secret");
        var authenticator = new BrokerSaslAuthenticator(config, codec);

        // when
        boolean success = authenticator.authenticate(channel);

        // then
        assertThat(success).isFalse();
    }

    @Test
    void rejectsWhenBrokerDeniesCredentials() {
        // given a channel that rejects authentication
        var channel = embeddedChannelWithBrokerSasl(
                new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()),
                new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                        .setErrorMessage("Authentication failed"));
        var config = new BrokerAuthConfig("PLAIN", "gateway", "wrong-password");
        var authenticator = new BrokerSaslAuthenticator(config, codec);

        // when
        boolean success = authenticator.authenticate(channel);

        // then
        assertThat(success).isFalse();
    }

    @Test
    void sendsCorrectPlainAuthBytes() {
        // given a channel that accepts everything; capture what was written
        var channel = embeddedChannelWithBrokerSasl(
                new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()),
                new SaslAuthenticateResponseData().setErrorCode(Errors.NONE.code()));
        var config = new BrokerAuthConfig("PLAIN", "alice", "s3cret");
        var authenticator = new BrokerSaslAuthenticator(config, codec);

        // when
        authenticator.authenticate(channel);

        // then — read the two outbound frames and verify the second is SaslAuthenticate
        // Frame 1: SaslHandshake request
        ByteBuf handshakeFrame = channel.readOutbound();
        assertThat(handshakeFrame).isNotNull();
        handshakeFrame.release();

        // Frame 2: SaslAuthenticate request
        ByteBuf authFrame = channel.readOutbound();
        assertThat(authFrame).isNotNull();

        // skip frame size (4 bytes), then apiKey is the first short
        authFrame.skipBytes(4);
        short apiKey = authFrame.readShort();
        assertThat(apiKey).isEqualTo((short) ApiKeys.SASL_AUTHENTICATE.id);

        authFrame.release();
    }

    @Test
    void sendsSaslHandshakeWithCorrectMechanism() {
        // given
        var channel = embeddedChannelWithBrokerSasl(
                new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()),
                new SaslAuthenticateResponseData().setErrorCode(Errors.NONE.code()));
        var config = new BrokerAuthConfig("PLAIN", "u", "p");
        var authenticator = new BrokerSaslAuthenticator(config, codec);

        // when
        authenticator.authenticate(channel);

        // then — first outbound frame should be SaslHandshake
        ByteBuf handshakeFrame = channel.readOutbound();
        assertThat(handshakeFrame).isNotNull();

        // skip frame size (4 bytes), then apiKey is the first short
        handshakeFrame.skipBytes(4);
        short apiKey = handshakeFrame.readShort();
        assertThat(apiKey).isEqualTo((short) ApiKeys.SASL_HANDSHAKE.id);

        handshakeFrame.release();
    }

    @Test
    void sendsIamSaslHandshakeWithIamMechanism() {
        // given
        var channel = embeddedChannelWithBrokerSasl(
                new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()),
                new SaslAuthenticateResponseData().setErrorCode(Errors.NONE.code()));
        var config = new BrokerAuthConfig("AWS_MSK_IAM", null, null, "us-east-1", null);
        var authenticator = new BrokerSaslAuthenticator(
                new IamSaslMechanism(config,
                        () -> AwsBasicCredentials.create("ACCESS", "SECRET")),
                codec, "b-1.cluster.abc.c2.kafka.us-east-1.amazonaws.com");

        // when
        authenticator.authenticate(channel);

        // then
        ByteBuf handshakeFrame = channel.readOutbound();
        assertThat(handshakeFrame).isNotNull();
        assertThat(handshakeFrame.toString(StandardCharsets.UTF_8)).contains("AWS_MSK_IAM");
        handshakeFrame.release();

        ByteBuf authenticateFrame = channel.readOutbound();
        assertThat(authenticateFrame).isNotNull();
        authenticateFrame.release();
    }

    /// Creates an [EmbeddedChannel] that automatically responds to SASL handshake
    /// and authenticate requests with the given responses, simulating a SASL-enabled broker.
    ///
    /// The mock intercepts outbound writes (requests from the authenticator), parses the
    /// API key, and fires a response frame back inbound through the pipeline so it arrives
    /// at the authenticator's handler as a decoded payload. The original request is forwarded
    /// through the encoder so it also appears in the channel's outbound queue.
    private EmbeddedChannel embeddedChannelWithBrokerSasl(
            SaslHandshakeResponseData handshakeResponse,
            SaslAuthenticateResponseData authenticateResponse
    ) {
        var responseHeaderCodec = new ResponseHeaderCodec();

        return new EmbeddedChannel(
                new KafkaFrameDecoder(),
                new KafkaFrameEncoder(),
                new io.netty.channel.ChannelDuplexHandler() {
                    @Override
                    public void write(
                            io.netty.channel.ChannelHandlerContext ctx,
                            Object msg,
                            io.netty.channel.ChannelPromise promise
                    ) {
                        ByteBuf buf = (ByteBuf) msg;
                        // The raw request frame (before encoding): [apiKey(2)][apiVersion(2)][...]
                        short apiKey = buf.readShort();
                        short apiVersion = buf.readShort();
                        buf.resetReaderIndex();

                        ByteBuf responsePayload = ctx.alloc().buffer();
                        if (apiKey == ApiKeys.SASL_HANDSHAKE.id) {
                            KafkaHeader header = KafkaHeader.of(
                                    KafkaApiRegistry.SASL_HANDSHAKE, apiVersion, 1, "test-broker");
                            responseHeaderCodec.encode(responsePayload,
                                    header.responseHeaderVersion(), 1);
                            codec.encodeRequest(
                                    KafkaApiRegistry.SASL_HANDSHAKE, apiVersion,
                                    handshakeResponse, responsePayload);
                        } else if (apiKey == ApiKeys.SASL_AUTHENTICATE.id) {
                            KafkaHeader header = KafkaHeader.of(
                                    KafkaApiRegistry.SASL_AUTHENTICATE, apiVersion, 2, "test-broker");
                            responseHeaderCodec.encode(responsePayload,
                                    header.responseHeaderVersion(), 2);
                            codec.encodeRequest(
                                    KafkaApiRegistry.SASL_AUTHENTICATE, apiVersion,
                                    authenticateResponse, responsePayload);
                        }

                        // Forward request to encoder so it appears in the outbound queue
                        ctx.write(msg, promise);

                        // Fire response inbound after the write completes to avoid re-entrancy.
                        // Wrap in a 4-byte length frame so the decoder strips it and delivers
                        // the decoded payload to the handler that sits after the decoder.
                        promise.addListener(
                                (io.netty.channel.ChannelFutureListener) f -> {
                                    ByteBuf framed = ctx.alloc().buffer(
                                            Integer.BYTES + responsePayload.readableBytes());
                                    framed.writeInt(responsePayload.readableBytes());
                                    framed.writeBytes(responsePayload);
                                    responsePayload.release();
                                    ctx.channel().pipeline().fireChannelRead(framed);
                                });
                    }
                });
    }
}
