package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.RequestHeaderCodec;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/// Performs the SASL handshake and PLAIN authentication against an upstream broker.
///
/// Used by `MetadataClient` and `BrokerClient` to authenticate the gateway's own
/// connection to the cluster when `BrokerAuthConfig` is present.
public final class BrokerSaslAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(BrokerSaslAuthenticator.class);
    private static final String HANDLER_NAME = "broker-sasl-auth";
    private static final int TIMEOUT_SECONDS = 10;

    private final BrokerAuthConfig config;
    private final KafkaBodyCodec codec;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final AtomicInteger correlationIds = new AtomicInteger();

    public BrokerSaslAuthenticator(BrokerAuthConfig config, KafkaBodyCodec codec) {
        this.config = config;
        this.codec = codec;
    }

    /// Performs SASL handshake + PLAIN authenticate on an already-connected channel.
    /// Returns `true` if the broker accepted both steps.
    public boolean authenticate(Channel channel) {
        try {
            SaslHandshakeResponseData handshakeResponse = doHandshake(channel);
            if (handshakeResponse.errorCode() != Errors.NONE.code()) {
                log.warn("Broker rejected SASL mechanism {}: error {}",
                        config.mechanism(), handshakeResponse.errorCode());
                return false;
            }
            SaslAuthenticateResponseData authResponse = doAuthenticate(channel);
            if (authResponse.errorCode() != Errors.NONE.code()) {
                log.warn("Broker rejected SASL credentials: error {}", authResponse.errorCode());
                return false;
            }
            log.info("SASL {} authentication succeeded against broker", config.mechanism());
            return true;
        } catch (Exception e) {
            log.error("SASL authentication to broker failed", e);
            return false;
        } finally {
            removeHandler(channel);
        }
    }

    private SaslHandshakeResponseData doHandshake(Channel channel) throws Exception {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        addHandler(channel, future);

        int correlationId = correlationIds.incrementAndGet();
        KafkaHeader header = KafkaHeader.of(
                KafkaApiRegistry.SASL_HANDSHAKE, (short) 1, correlationId, "kawa-gateway");
        ByteBuf out = channel.alloc().buffer();
        requestHeaderCodec.encode(out, header);
        codec.encodeRequest(KafkaApiRegistry.SASL_HANDSHAKE, (short) 1,
                new SaslHandshakeRequestData().setMechanism(config.mechanism()), out);
        channel.writeAndFlush(out);

        ByteBuf frame = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            short responseHeaderVersion = KafkaHeader.of(
                    KafkaApiRegistry.SASL_HANDSHAKE, (short) 1, 0, null).responseHeaderVersion();
            responseHeaderCodec.decodeCorrelationId(frame, responseHeaderVersion);
            return (SaslHandshakeResponseData) codec.decodeResponse(
                    KafkaApiRegistry.SASL_HANDSHAKE, (short) 1, frame);
        } finally {
            frame.release();
        }
    }

    private SaslAuthenticateResponseData doAuthenticate(Channel channel) throws Exception {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        replaceHandler(channel, future);

        int correlationId = correlationIds.incrementAndGet();
        KafkaHeader header = KafkaHeader.of(
                KafkaApiRegistry.SASL_AUTHENTICATE, (short) 2, correlationId, "kawa-gateway");
        byte[] authBytes = ("\0" + config.username() + "\0" + config.password())
                .getBytes(StandardCharsets.UTF_8);
        ByteBuf out = channel.alloc().buffer();
        requestHeaderCodec.encode(out, header);
        codec.encodeRequest(KafkaApiRegistry.SASL_AUTHENTICATE, (short) 2,
                new SaslAuthenticateRequestData().setAuthBytes(authBytes), out);
        channel.writeAndFlush(out);

        ByteBuf frame = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            short responseHeaderVersion = KafkaHeader.of(
                    KafkaApiRegistry.SASL_AUTHENTICATE, (short) 2, 0, null).responseHeaderVersion();
            responseHeaderCodec.decodeCorrelationId(frame, responseHeaderVersion);
            return (SaslAuthenticateResponseData) codec.decodeResponse(
                    KafkaApiRegistry.SASL_AUTHENTICATE, (short) 2, frame);
        } finally {
            frame.release();
        }
    }

    private static void addHandler(Channel channel, CompletableFuture<ByteBuf> future) {
        ChannelInboundHandlerAdapter handler = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof ByteBuf buf) {
                    future.complete(buf.retainedDuplicate());
                } else {
                    ctx.fireChannelRead(msg);
                }
            }
        };
        // After the Kafka frame decoder, so the handler reads whole frames; a TLS handler before it is
        // also a ByteToMessageDecoder, so look the frame decoder up by its own type.
        String decoderName = null;
        ChannelHandlerContext decoderCtx = channel.pipeline().context(KafkaFrameDecoder.class);
        if (decoderCtx != null) {
            decoderName = decoderCtx.name();
        }
        if (decoderName != null) {
            channel.pipeline().addAfter(decoderName, HANDLER_NAME, handler);
        } else {
            channel.pipeline().addFirst(HANDLER_NAME, handler);
        }
    }

    private static void replaceHandler(Channel channel, CompletableFuture<ByteBuf> future) {
        channel.pipeline().remove(HANDLER_NAME);
        addHandler(channel, future);
    }

    private static void removeHandler(Channel channel) {
        if (channel.pipeline().get(HANDLER_NAME) != null) {
            channel.pipeline().remove(HANDLER_NAME);
        }
    }
}
