package io.jonasg.kawa.server;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.Route;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.ApiVersionsResponseBuilder;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.server.broker.BrokerClientPool;
import io.jonasg.kawa.server.broker.MetadataClient;
import io.jonasg.kawa.server.auth.AuthenticationResult;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Handles a single decoded client frame: answers ApiVersions and SASL bootstrap APIs locally,
/// otherwise runs the interceptor pipeline, routes to the right broker and forwards the request.
public final class KafkaClientRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaClientRequestHandler.class);

    private final KafkaBodyCodec codec;
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final ApiVersionsResponseBuilder apiVersionsBuilder;
    private final InterceptorPipeline pipeline;
    private final LeaderRouter router;
    private final BrokerClientPool brokerPool;
    private final MetadataClient metadataClient;
    private final GatewayMetrics metrics;
    private final FetchSessionRegistry fetchSessions;
    private final SaslAuthenticator saslAuthenticator;

    public KafkaClientRequestHandler(
            KafkaBodyCodec codec,
            ApiVersionsResponseBuilder apiVersionsBuilder,
            InterceptorPipeline pipeline,
            LeaderRouter router,
            BrokerClientPool brokerPool,
            MetadataClient metadataClient,
            GatewayMetrics metrics,
            FetchSessionRegistry fetchSessions,
            SaslAuthenticator saslAuthenticator) {
        this.codec = codec;
        this.apiVersionsBuilder = apiVersionsBuilder;
        this.pipeline = pipeline;
        this.router = router;
        this.brokerPool = brokerPool;
        this.metadataClient = metadataClient;
        this.metrics = metrics;
        this.fetchSessions = fetchSessions;
        this.saslAuthenticator = saslAuthenticator;
    }

    public void handleRequest(
            ClientSession session,
            KafkaClientRequest request
    ) {
        session.requestReceived(request.correlationId());
        switch (request.apiKey()) {
            case KafkaApiRegistry.API_VERSIONS -> handleApiVersions(session, request.header());
            case KafkaApiRegistry.SASL_HANDSHAKE -> handleSaslHandshake(session, request);
            case KafkaApiRegistry.SASL_AUTHENTICATE -> handleSaslAuthenticate(session, request)
                    .onSuccess(session::setPrincipal);
            default -> maybeIntercept(session, request);
        }
    }

    private void maybeIntercept(
            ClientSession session,
            KafkaClientRequest request
    ) {
        var context = new GatewayContext(session, System.nanoTime(), session.principal());
        pipeline.onRequest(context, request);
        if (context.isShortCircuited()) {
            writeShortCircuit(session, request, context.shortCircuitResult());
            return;
        }
        Route route = router.route(request);
        context.route(route);
        metrics.request(request.apiName(), "routed");

        session.requestInFlight();
        brokerPool.forBroker(route.brokerId()).send(session, context, request);
    }

    private void handleApiVersions(
            ClientSession session,
            KafkaHeader header
    ) {
        short requestVersion = header.apiVersion();
        var result = apiVersionsBuilder.build(requestVersion, metadataClient.brokerRanges());
        short responseHeaderVersion = KafkaHeader.of(
                        KafkaApiRegistry.API_VERSIONS, result.responseVersion(), header.correlationId(), null)
                .responseHeaderVersion();

        ByteBuf out = session.channel().alloc().buffer();
        responseHeaderCodec.encode(out, responseHeaderVersion, header.correlationId());
        codec.encodeResponse(KafkaApiRegistry.API_VERSIONS, result.responseVersion(), result.data(), out);
        session.writeResponse(header.correlationId(), out);
        metrics.request("ApiVersions", "answered-locally");
        metrics.response("ApiVersions", "ok");
    }

    /// Answers SaslHandshake locally, the way a real broker does: the gateway decides whether
    /// the requested mechanism is supported and writes the response itself - no broker round
    /// trip. The response version mirrors the request version.
    private void handleSaslHandshake(
            ClientSession session,
            KafkaClientRequest request
    ) {
        var data = (SaslHandshakeRequestData) request.body();
        log.info("SaslHandshake request from {}: mechanism={}", session, data.mechanism());

        var response = saslAuthenticator.handleHandshake(session, (SaslHandshakeRequestData) request.body());
        short version = request.apiVersion();
        short responseHeaderVersion = KafkaHeader.of(
                        KafkaApiRegistry.SASL_HANDSHAKE, version, request.correlationId(), null)
                .responseHeaderVersion();

        ByteBuf out = session.channel().alloc().buffer();
        responseHeaderCodec.encode(out, responseHeaderVersion, request.correlationId());
        codec.encodeResponse(KafkaApiRegistry.SASL_HANDSHAKE, version, response, out);
        session.writeResponse(request.correlationId(), out);
        metrics.request("SaslHandshake", "answered-locally");
        metrics.response("SaslHandshake", "ok");
    }

    /// Answers SaslAuthenticate locally by validating the supplied auth bytes against the
    /// gateway's own user directory.
    private AuthenticationResult handleSaslAuthenticate(
            ClientSession session,
            KafkaClientRequest request
    ) {
        var requestData = (SaslAuthenticateRequestData) request.body();

        log.info("SaslAuthenticate request from {}: {} auth bytes", session, requestData.authBytes().length);

        var result = saslAuthenticator.handleAuthenticate(session, requestData);
        var response = result.response();
        short version = request.apiVersion();
        short responseHeaderVersion = KafkaHeader.of(
                        KafkaApiRegistry.SASL_AUTHENTICATE, version, request.correlationId(), null)
                .responseHeaderVersion();

        ByteBuf out = session.channel().alloc().buffer();
        responseHeaderCodec.encode(out, responseHeaderVersion, request.correlationId());
        codec.encodeResponse(KafkaApiRegistry.SASL_AUTHENTICATE, version, response, out);
        session.writeResponse(request.correlationId(), out);
        metrics.request("SaslAuthenticate", "answered-locally");
        metrics.response("SaslAuthenticate", result instanceof AuthenticationResult.Failure ? "error" : "ok");
        return result;
    }

    private void writeShortCircuit(
            ClientSession session,
            KafkaClientRequest request,
            ShortCircuitResult result
    ) {
        short responseHeaderVersion = KafkaHeader.of(
                        result.apiKey(), result.apiVersion(), request.correlationId(), null)
                .responseHeaderVersion();

        ByteBuf out = session.channel().alloc().buffer();
        responseHeaderCodec.encode(out, responseHeaderVersion, request.correlationId());
        codec.encodeResponse(result.apiKey(), result.apiVersion(), result.body(), out);
        session.writeResponse(request.correlationId(), out);
        metrics.request(request.apiName(), "short-circuited");
        metrics.response(request.apiName(), "ok");
    }

    public void sessionClosed(ClientSession session) {
        brokerPool.closeSession(session);
        fetchSessions.sessionClosed(session);
        saslAuthenticator.sessionClosed(session);
    }
}
