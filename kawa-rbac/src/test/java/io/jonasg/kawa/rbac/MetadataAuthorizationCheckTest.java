package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataAuthorizationCheckTest {

    private static RbacAuthorizer authorizer(AclConfig... acls) {
        return new RbacAuthorizer(new RbacConfig(
                Map.of("admin", new RoleConfig(List.of(acls))),
                Map.of("admins", new GroupConfig(List.of("alice"), List.of("admin")))));
    }

    private static AclConfig topicAcl(String topic, AclOperation operation) {
        return new AclConfig(new ResourceConfig(ResourceType.TOPIC, topic), operation);
    }

    private static GatewayContext context(String principal) {
        return new GatewayContext("source", 0L, principal);
    }

    private static Request metadataRequest(String... topics) {
        var body = new MetadataRequestData();
        if (topics.length > 0) {
            for (String topic : topics) {
                body.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(topic));
            }
        } else {
            body.setTopics(null); // list-all
        }
        return new TestRequest(ApiKeys.METADATA.id, (short) 12, body);
    }

    private static Request metadataRequest(MetadataRequestData body) {
        return new TestRequest(ApiKeys.METADATA.id, (short) 12, body);
    }

    private static Response metadataResponse(MetadataResponseData body) {
        return new TestResponse(ApiKeys.METADATA.id, body);
    }

    private static MetadataResponseData brokerResponse(String... topics) {
        var body = new MetadataResponseData();
        for (String topic : topics) {
            body.topics().add(new MetadataResponseData.MetadataResponseTopic()
                    .setName(topic).setErrorCode((short) 0));
        }
        return body;
    }

    @Test
    void stripsDeniedNamedTopicAndMergesUnknownTopicIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = metadataRequest("orders", "banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (MetadataRequestData) request.body();
        assertThat(body.topics()).hasSize(1);
        assertThat(body.topics().iterator().next().name()).isEqualTo("orders");

        var response = metadataResponse(brokerResponse("orders"));
        interceptor.onResponse(ctx, response);

        var responseBody = (MetadataResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void shortCircuitsMetadataWhenEveryNamedTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = metadataRequest("banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(MetadataResponseData.class);
        var response = (MetadataResponseData) result.body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().name()).isEqualTo("banned");
        assertThat(response.topics().iterator().next().errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void filtersListAllResponseToOnlyDescribableTopics() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = metadataRequest(); // list-all, topics() == null

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();

        var response = metadataResponse(brokerResponse("orders", "banned"));
        interceptor.onResponse(ctx, response);

        var responseBody = (MetadataResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(1);
        assertThat(responseBody.topics().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void filtersListAllResponseAgainstVirtualNameForVirtualTopic() {
        // The broker returns the physical name; the check must translate it to the virtual name
        // (via VirtualTopicManager) before checking the ACL, since this runs before
        // VirtualTopicInterceptor's rename step.
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-physical"))));
        var ctx = context("alice");
        var request = metadataRequest(); // list-all

        interceptor.onRequest(ctx, request);

        var response = metadataResponse(brokerResponse("orders-physical", "banned"));
        interceptor.onResponse(ctx, response);

        var responseBody = (MetadataResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(1);
        assertThat(responseBody.topics().iterator().next().name()).isEqualTo("orders-physical");
    }

    @Test
    void synthesizedDenialEntrySurvivesListAllRemoveIfPass() {
        // Regression for the ordering bug: the synthesized UNKNOWN_TOPIC_OR_PARTITION entries
        // from the specific-topics path must be appended AFTER the list-all removeIf pass, or
        // removeIf would delete them again (the principal is by definition denied DESCRIBE on
        // them). This test proves the correct ordering.
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = metadataRequest("orders", "banned");

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();

        var response = metadataResponse(brokerResponse("orders"));
        interceptor.onResponse(ctx, response);

        var responseBody = (MetadataResponseData) response.body();
        assertThat(responseBody.topics()).hasSize(2);
        var banned = responseBody.topics().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    private record TestRequest(int apiKey, short apiVersion, Object body) implements Request {
        @Override
        public String apiName() {
            return "test";
        }

        @Override
        public int correlationId() {
            return 42;
        }

        @Override
        public String clientId() {
            return "test";
        }
    }

    private record TestResponse(int apiKey, Object body) implements Response {
        @Override
        public String apiName() {
            return "test";
        }

        @Override
        public short apiVersion() {
            return 12;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}
