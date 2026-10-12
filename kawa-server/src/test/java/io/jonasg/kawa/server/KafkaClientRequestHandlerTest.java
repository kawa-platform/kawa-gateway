package io.jonasg.kawa.server;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.ApiVersionsResponseBuilder;
import io.jonasg.kawa.protocol.kafka.ByteBufReadable;
import io.jonasg.kawa.protocol.kafka.ByteBufWritable;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.RequestHeaderCodec;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.jonasg.kawa.protocol.kafka.SupportedVersions;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.server.broker.BrokerClientPool;
import io.jonasg.kawa.server.broker.MetadataClient;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.server.netty.ClientSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClientRequestHandlerTest {

    private final KafkaApiRegistry registry = KafkaApiRegistry.create();
    private final KafkaBodyCodec codec = new KafkaBodyCodec(registry);
    private final ApiVersionsResponseBuilder apiVersionsBuilder =
            new ApiVersionsResponseBuilder(SupportedVersions.from(registry));
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());

    @Test
    void answersApiVersionsLocally() {
        KafkaClientRequestHandler dispatcher = dispatcher();
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel);

        dispatcher.handleRequest(session, apiVersionsRequest(3));

        ByteBuf encoded = channel.readOutbound();
        assertThat(encoded).isNotNull();

        ByteBuf frame = encoded.copy();
        frame.readInt();
        int corrId = responseHeaderCodec.decodeCorrelationId(frame, (short) 0);
        assertThat(corrId).isEqualTo((short) 42);

        Object body = codec.decodeResponse(
                KafkaApiRegistry.API_VERSIONS, (short) 3, frame);
        assertThat(body).isInstanceOf(ApiVersionsResponseData.class);
        ApiVersionsResponseData data = (ApiVersionsResponseData) body;
        assertThat(data.errorCode()).isZero();
        assertThat(data.apiKeys()).extracting(ApiVersionsResponseData.ApiVersion::apiKey)
                .contains((short) KafkaApiRegistry.API_VERSIONS, (short) KafkaApiRegistry.METADATA,
                        (short) KafkaApiRegistry.PRODUCE, (short) KafkaApiRegistry.FETCH);
        assertThat(data.apiKeys()).extracting(ApiVersionsResponseData.ApiVersion::maxVersion)
                .contains((short) 8);

        frame.release();
        encoded.release();
    }

    @Test
    void answersApiVersionsAtMaxSupportedWhenClientRequestsNewer() {
        KafkaClientRequestHandler dispatcher = dispatcher();
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel);

        dispatcher.handleRequest(session, apiVersionsRequest(5));

        ByteBuf encoded = channel.readOutbound();
        assertThat(encoded).isNotNull();

        ByteBuf frame = encoded.copy();
        frame.readInt();
        responseHeaderCodec.decodeCorrelationId(frame, (short) 0);
        Object body = codec.decodeResponse(
                KafkaApiRegistry.API_VERSIONS, (short) 3, frame);
        ApiVersionsResponseData data = (ApiVersionsResponseData) body;
        assertThat(data.errorCode()).isEqualTo((short) 35);

        frame.release();
        encoded.release();
    }

    @Test
    void returnsShortCircuitedCreateTopicsResponseWithoutForwarding() {
        var localResponse = new CreateTopicsResponseData();
        localResponse.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
                .setName("orders")
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("virtual topic 'orders' is reserved; use 'orders-v2'"));

        var shortCircuitingInterceptor = new Interceptor() {
            @Override
            public void onRequest(
                    GatewayContext context,
                    Request request
            ) {
                context.shortCircuit(
                        new ShortCircuitResult((short) ApiKeys.CREATE_TOPICS.id, (short) 7, localResponse));
            }
        };

        KafkaClientRequestHandler dispatcher = dispatcher(List.of(shortCircuitingInterceptor));
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel);

        dispatcher.handleRequest(session, createTopicsRequest("orders", (short) 7));

        ByteBuf encoded = channel.readOutbound();
        assertThat(encoded).isNotNull();

        ByteBuf frame = encoded.copy();
        frame.readInt();
        int corrId = responseHeaderCodec.decodeCorrelationId(frame,
                KafkaHeader.of((short) ApiKeys.CREATE_TOPICS.id, (short) 7, 42, "test").responseHeaderVersion());
        assertThat(corrId).isEqualTo(42);

        var responseData = new CreateTopicsResponseData(
                new ByteBufReadable(frame), (short) 7);
        assertThat(responseData.topics()).hasSize(1);
        CreateTopicsResponseData.CreatableTopicResult result = responseData.topics().iterator().next();
        assertThat(result.name()).isEqualTo("orders");
        assertThat(result.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());

        frame.release();
        encoded.release();
    }

    @Test
    void answersSaslAuthenticateLocally() {
        // given
        KafkaClientRequestHandler dispatcher = dispatcher();
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel, SecurityProtocol.SASL_PLAINTEXT);

        // when
        dispatcher.handleRequest(session, saslHandshakeRequest("PLAIN", (short) 1));
        channel.readOutbound(); // discard the handshake response
        dispatcher.handleRequest(session, saslAuthenticateRequest("\u0000alice\u0000secret", (short) 2));

        // then
        ByteBuf encoded = channel.readOutbound();
        assertThat(encoded).isNotNull();

        ByteBuf frame = encoded.copy();
        frame.readInt();
        int corrId = responseHeaderCodec.decodeCorrelationId(frame,
                KafkaHeader.of((short) ApiKeys.SASL_AUTHENTICATE.id, (short) 2, 42, "test").responseHeaderVersion());
        assertThat(corrId).isEqualTo(42);

        Object body = codec.decodeResponse(KafkaApiRegistry.SASL_AUTHENTICATE, (short) 2, frame);
        assertThat(body).isInstanceOf(SaslAuthenticateResponseData.class);
        var response = (SaslAuthenticateResponseData) body;
        assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());

        frame.release();
        encoded.release();
    }

    @Test
    void closesASaslConnectionThatSendsARequestBeforeAuthenticating() {
        // given
        KafkaClientRequestHandler dispatcher = dispatcher(List.of(shortCircuitingCreateTopics()));
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel, SecurityProtocol.SASL_PLAINTEXT);

        // when
        dispatcher.handleRequest(session, createTopicsRequest("orders", (short) 7));

        // then
        assertThat(channel.<ByteBuf>readOutbound())
                .withFailMessage(() -> "A request before SASL authentication was answered")
                .isNull();
        assertThat(channel.isOpen())
                .withFailMessage(() -> "The connection was not closed after a request before SASL authentication")
                .isFalse();
    }

    @Test
    void admitsRequestsOnASaslConnectionOnceAuthenticated() {
        // given
        KafkaClientRequestHandler dispatcher = dispatcher(List.of(shortCircuitingCreateTopics()));
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel, SecurityProtocol.SASL_SSL);
        dispatcher.handleRequest(session, saslHandshakeRequest("PLAIN", (short) 1));
        dispatcher.handleRequest(session, saslAuthenticateRequest("\u0000alice\u0000secret", (short) 2));
        release(channel.readOutbound());
        release(channel.readOutbound());

        // when
        dispatcher.handleRequest(session, createTopicsRequest("orders", (short) 7));

        // then
        ByteBuf encoded = channel.readOutbound();
        assertThat(encoded)
                .withFailMessage(() -> "An authenticated request was not answered")
                .isNotNull();
        assertThat(channel.isOpen()).isTrue();
        encoded.release();
    }

    @Test
    void answersSaslHandshakeWithIllegalSaslStateOnANonSaslListener() {
        // given
        KafkaClientRequestHandler dispatcher = dispatcher();
        var channel = new EmbeddedChannel(new KafkaFrameEncoder());
        var session = new ClientSession(channel, SecurityProtocol.SSL);

        // when
        dispatcher.handleRequest(session, saslHandshakeRequest("PLAIN", (short) 1));

        // then
        ByteBuf encoded = channel.readOutbound();
        ByteBuf frame = encoded.copy();
        frame.readInt();
        responseHeaderCodec.decodeCorrelationId(frame,
                KafkaHeader.of((short) ApiKeys.SASL_HANDSHAKE.id, (short) 1, 42, "test").responseHeaderVersion());
        var response = (SaslHandshakeResponseData) codec.decodeResponse(KafkaApiRegistry.SASL_HANDSHAKE, (short) 1, frame);
        assertThat(response.errorCode())
                .withFailMessage(() -> "SaslHandshake on an SSL listener was not refused with ILLEGAL_SASL_STATE")
                .isEqualTo(Errors.ILLEGAL_SASL_STATE.code());

        frame.release();
        encoded.release();
    }

    private static Interceptor shortCircuitingCreateTopics() {
        return new Interceptor() {
            @Override
            public void onRequest(
                    GatewayContext context,
                    Request request
            ) {
                context.shortCircuit(new ShortCircuitResult((short) ApiKeys.CREATE_TOPICS.id, (short) 7, new CreateTopicsResponseData()));
            }
        };
    }

    private static void release(ByteBuf buf) {
        if (buf != null) {
            buf.release();
        }
    }

    private KafkaClientRequestHandler dispatcher() {
        return dispatcher(List.of());
    }

    private KafkaClientRequestHandler dispatcher(List<Interceptor> interceptors) {
        var group = new NioEventLoopGroup(1);
        var cache = new MetadataCache();
        var pipeline = new InterceptorPipeline(interceptors);
        var brokerPool = new BrokerClientPool(
                group, codec, pipeline, metrics, cache, "localhost", 9092);
        MetadataClient metadataClient =
                new MetadataClient("localhost", 9092, group, codec, cache, brokerPool, metrics);
        return new KafkaClientRequestHandler(
                codec, apiVersionsBuilder, pipeline,
                new LeaderRouter(cache), brokerPool, metadataClient, metrics, new FetchSessionRegistry(),
                new SaslAuthenticator(
                        Set.of("PLAIN"),
                        java.util.Map.of("alice", new ClientConfig("PLAIN",
                                HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret")))));
    }

    private KafkaClientRequest apiVersionsRequest(int version) {
        var body = new ApiVersionsRequestData();
        KafkaHeader header = KafkaHeader.of(
                KafkaApiRegistry.API_VERSIONS, (short) version, (short) 42, "test");
        return KafkaClientRequest.of(header, body);
    }

    private KafkaClientRequest createTopicsRequest(
            String topic,
            short version
    ) {
        var body = new CreateTopicsRequestData();
        body.topics().add(new CreateTopicsRequestData.CreatableTopic()
                .setName(topic)
                .setNumPartitions(1)
                .setReplicationFactor((short) 1));
        KafkaHeader header = KafkaHeader.of((short) ApiKeys.CREATE_TOPICS.id, version, 42, "test");
        return KafkaClientRequest.of(header, body);
    }

    private KafkaClientRequest saslHandshakeRequest(
            String mechanism,
            short version
    ) {
        var body = new SaslHandshakeRequestData().setMechanism(mechanism);
        KafkaHeader header = KafkaHeader.of((short) ApiKeys.SASL_HANDSHAKE.id, version, 42, "test");
        return KafkaClientRequest.of(header, body);
    }

    private KafkaClientRequest saslAuthenticateRequest(
            String plainPayload,
            short version
    ) {
        var body = new SaslAuthenticateRequestData().setAuthBytes(plainPayload.getBytes());
        KafkaHeader header = KafkaHeader.of((short) ApiKeys.SASL_AUTHENTICATE.id, version, 42, "test");
        return KafkaClientRequest.of(header, body);
    }

    private ByteBuf apiVersionsFrame(int version) {
        var body = new ApiVersionsRequestData();
        KafkaHeader header = KafkaHeader.of(
                KafkaApiRegistry.API_VERSIONS, (short) version, (short) 42, "test");
        ByteBuf payload = Unpooled.buffer();
        requestHeaderCodec.encode(payload, header);
        var cache = new ObjectSerializationCache();
        int bodySize = body.size(cache, (short) version);
        payload.writeInt(bodySize);
        body.write(new ByteBufWritable(payload), cache, (short) version);
        return payload;
    }
}
