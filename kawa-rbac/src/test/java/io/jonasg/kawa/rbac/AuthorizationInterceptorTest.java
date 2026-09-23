package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuthorizationInterceptorTest {

    private static final String CLUSTER = "kafka-cluster";

    private static RbacAuthorizer authorizer(AclConfig... acls) {
        return new RbacAuthorizer(new RbacConfig(
                Map.of("admin", new RoleConfig(List.of(acls))),
                Map.of("admins", new GroupConfig(List.of("alice"), List.of("admin")))));
    }

    private static AclConfig clusterAcl(AclOperation operation) {
        return new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), operation);
    }

    private static GatewayContext context(String principal) {
        return new GatewayContext("source", 0L, principal);
    }

    private static Request createTopicsRequest() {
        var body = new CreateTopicsRequestData();
        body.topics().add(new CreateTopicsRequestData.CreatableTopic()
                .setName("orders").setNumPartitions(1).setReplicationFactor((short) 1));
        return new TestRequest(ApiKeys.CREATE_TOPICS.id, (short) 7, body);
    }

    private static Request deleteTopicsRequest() {
        var body = new DeleteTopicsRequestData();
        body.topicNames().add("orders");
        return new TestRequest(ApiKeys.DELETE_TOPICS.id, (short) 7, body);
    }

    private static Request describeLogDirsRequest() {
        return new TestRequest(ApiKeys.DESCRIBE_LOG_DIRS.id, (short) 4, new DescribeLogDirsRequestData());
    }

    private static Request describeAclsRequest() {
        return new TestRequest(ApiKeys.DESCRIBE_ACLS.id, (short) 3, new DescribeAclsRequestData());
    }

    private static Request createAclsRequest() {
        var body = new CreateAclsRequestData();
        body.creations().add(new CreateAclsRequestData.AclCreation()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName("orders")
                .setResourcePatternType(PatternType.LITERAL.code())
                .setPrincipal("User:alice")
                .setHost("*")
                .setOperation(AclOperation.READ.code())
                .setPermissionType(AclPermissionType.ALLOW.code()));
        return new TestRequest(ApiKeys.CREATE_ACLS.id, (short) 3, body);
    }

    private static Request deleteAclsRequest() {
        var body = new DeleteAclsRequestData();
        body.filters().add(new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.TOPIC.code())
                .setResourceNameFilter("orders")
                .setPrincipalFilter("User:alice")
                .setHostFilter("*")
                .setOperation(AclOperation.READ.code())
                .setPermissionType(AclPermissionType.ALLOW.code()));
        return new TestRequest(ApiKeys.DELETE_ACLS.id, (short) 3, body);
    }

    private static Request findCoordinatorRequest(byte keyType, String key) {
        return new TestRequest(ApiKeys.FIND_COORDINATOR.id, (short) 2,
                new FindCoordinatorRequestData().setKeyType(keyType).setKey(key));
    }

    private static Request joinGroupRequest() {
        return new TestRequest(ApiKeys.JOIN_GROUP.id, (short) 9,
                new JoinGroupRequestData().setGroupId("orders-group"));
    }

    private static Request syncGroupRequest() {
        return new TestRequest(ApiKeys.SYNC_GROUP.id, (short) 5,
                new SyncGroupRequestData().setGroupId("orders-group"));
    }

    private static Request heartbeatRequest() {
        return new TestRequest(ApiKeys.HEARTBEAT.id, (short) 4,
                new HeartbeatRequestData().setGroupId("orders-group"));
    }

    private static Request leaveGroupRequest() {
        return new TestRequest(ApiKeys.LEAVE_GROUP.id, (short) 5,
                new LeaveGroupRequestData().setGroupId("orders-group"));
    }

    private static AclConfig groupAcl(String group) {
        return new AclConfig(new ResourceConfig(ResourceType.GROUP, group), AclOperation.READ);
    }

    private static AclConfig groupAcl(String group, AclOperation operation) {
        return new AclConfig(new ResourceConfig(ResourceType.GROUP, group), operation);
    }

    private static AclConfig topicAcl(String topic, AclOperation operation) {
        return new AclConfig(new ResourceConfig(ResourceType.TOPIC, topic), operation);
    }

    private static AclConfig transactionalIdAcl(String transactionalId, AclOperation operation) {
        return new AclConfig(new ResourceConfig(ResourceType.TRANSACTIONAL_ID, transactionalId), operation);
    }

    private static Request produceRequest(String topic, int... partitions) {
        var body = new ProduceRequestData();
        var topicData = new ProduceRequestData.TopicProduceData().setName(topic);
        for (int partition : partitions) {
            topicData.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(partition));
        }
        body.topicData().add(topicData);
        return new TestRequest(ApiKeys.PRODUCE.id, (short) 9, body);
    }

    private static Request produceRequest(ProduceRequestData body) {
        return new TestRequest(ApiKeys.PRODUCE.id, (short) 9, body);
    }

    private static Response produceResponse(ProduceResponseData body) {
        return new TestResponse(ApiKeys.PRODUCE.id, body);
    }

    private static ProduceResponseData brokerResponse(String topic, int... partitions) {
        var body = new ProduceResponseData();
        var topicResponse = new ProduceResponseData.TopicProduceResponse().setName(topic);
        for (int partition : partitions) {
            topicResponse.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(partition).setErrorCode((short) 0).setBaseOffset(42L));
        }
        body.responses().add(topicResponse);
        return body;
    }

    @Test
    void shortCircuitsCreateTopicsWhenClusterCreateIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, createTopicsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.apiKey()).isEqualTo((short) ApiKeys.CREATE_TOPICS.id);
        assertThat(result.body()).isInstanceOf(CreateTopicsResponseData.class);
        var response = (CreateTopicsResponseData) result.body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().errorCode())
                .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsCreateTopicsWhenClusterCreateIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(clusterAcl(AclOperation.CREATE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, createTopicsRequest());

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsDeleteTopicsWhenClusterDeleteIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, deleteTopicsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DeleteTopicsResponseData.class);
        var response = (DeleteTopicsResponseData) result.body();
        assertThat(response.responses()).hasSize(1);
        assertThat(response.responses().iterator().next().errorCode())
                .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsDescribeLogDirsWhenClusterDescribeIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, describeLogDirsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DescribeLogDirsResponseData.class);
        var response = (DescribeLogDirsResponseData) result.body();
        assertThat(response.errorCode()).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsDescribeLogDirsWhenClusterDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(clusterAcl(AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, describeLogDirsRequest());

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsDescribeAclsWhenClusterDescribeIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, describeAclsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DescribeAclsResponseData.class);
        var response = (DescribeAclsResponseData) result.body();
        assertThat(response.errorCode()).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsDescribeAclsWhenClusterDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(clusterAcl(AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, describeAclsRequest());

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsCreateAclsWhenClusterAlterIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, createAclsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(CreateAclsResponseData.class);
        var response = (CreateAclsResponseData) result.body();
        assertThat(response.results()).hasSize(1);
        assertThat(response.results().iterator().next().errorCode())
                .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsCreateAclsWhenClusterAlterIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(clusterAcl(AclOperation.ALTER)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, createAclsRequest());

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsDeleteAclsWhenClusterAlterIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, deleteAclsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(DeleteAclsResponseData.class);
        var response = (DeleteAclsResponseData) result.body();
        assertThat(response.filterResults()).hasSize(1);
        assertThat(response.filterResults().iterator().next().errorCode())
                .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsDeleteAclsWhenClusterAlterIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(clusterAcl(AclOperation.ALTER)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, deleteAclsRequest());

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsFindCoordinatorWhenGroupDescribeIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, findCoordinatorRequest((byte) 0, "orders-group"));

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(FindCoordinatorResponseData.class);
        var response = (FindCoordinatorResponseData) result.body();
        assertThat(response.errorCode()).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsFindCoordinatorWhenGroupDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(groupAcl("orders-group", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, findCoordinatorRequest((byte) 0, "orders-group"));

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsFindCoordinatorWhenTransactionalIdDescribeIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, findCoordinatorRequest((byte) 1, "orders-txn"));

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(FindCoordinatorResponseData.class);
        var response = (FindCoordinatorResponseData) result.body();
        assertThat(response.errorCode()).isEqualTo(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsFindCoordinatorWhenTransactionalIdDescribeIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(transactionalIdAcl("orders-txn", AclOperation.DESCRIBE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, findCoordinatorRequest((byte) 1, "orders-txn"));

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void doesNotGateApisOutsideTheWholeRequestClusterSlice() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.API_VERSIONS.id, (short) 3, new Object());

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void deniesGatedApiWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);

        interceptor.onRequest(ctx, createTopicsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(CreateTopicsResponseData.class);
        var response = (CreateTopicsResponseData) result.body();
        assertThat(response.topics()).hasSize(1);
        assertThat(response.topics().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void stopsThePipelineAfterShortCircuitingSoLaterInterceptorsNeverRun() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var events = new ArrayList<String>();
        Interceptor later = new Interceptor() {
            @Override
            public void onRequest(GatewayContext context, Request request) {
                events.add("later.onRequest");
            }

            @Override
            public void onResponse(GatewayContext context, Response response) {
                events.add("later.onResponse");
            }
        };
        var pipeline = new InterceptorPipeline(List.of(interceptor, later));
        var ctx = context("alice");

        pipeline.onRequest(ctx, createTopicsRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        assertThat(events).isEmpty();
    }

    @Test
    void shortCircuitsJoinGroupWhenGroupReadIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, joinGroupRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(JoinGroupResponseData.class);
        assertThat(((JoinGroupResponseData) result.body()).errorCode())
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsJoinGroupWhenGroupReadIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(groupAcl("orders-group")),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, joinGroupRequest());

        assertThat(ctx.isShortCircuited()).isFalse();
    }

    @Test
    void shortCircuitsSyncGroupWhenGroupReadIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, syncGroupRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        assertThat(((SyncGroupResponseData) ctx.shortCircuitResult().body()).errorCode())
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsHeartbeatWhenGroupReadIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, heartbeatRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        assertThat(((HeartbeatResponseData) ctx.shortCircuitResult().body()).errorCode())
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void shortCircuitsLeaveGroupWhenGroupReadIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");

        interceptor.onRequest(ctx, leaveGroupRequest());

        assertThat(ctx.isShortCircuited()).isTrue();
        assertThat(((LeaveGroupResponseData) ctx.shortCircuitResult().body()).errorCode())
                .isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }

    @Test
    void allowsProduceWhenTopicWriteIsAllowed() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.WRITE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = produceRequest("orders", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        var body = (ProduceRequestData) request.body();
        assertThat(body.topicData()).hasSize(1);
        assertThat(body.topicData().iterator().next().name()).isEqualTo("orders");
    }

    @Test
    void stripsDeniedTopicsAndMergesDenialIntoResponse() {
        var interceptor = new AuthorizationInterceptor(
                authorizer(topicAcl("orders", AclOperation.WRITE)),
                new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var body = new ProduceRequestData();
        body.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0))));
        body.topicData().add(new ProduceRequestData.TopicProduceData().setName("banned")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(3))));
        var request = produceRequest(body);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(body.topicData()).hasSize(1);
        assertThat(body.topicData().iterator().next().name()).isEqualTo("orders");

        var response = produceResponse(brokerResponse("orders", 0));
        interceptor.onResponse(ctx, response);

        var responseBody = (ProduceResponseData) response.body();
        assertThat(responseBody.responses()).hasSize(2);
        var banned = responseBody.responses().stream()
                .filter(t -> t.name().equals("banned")).findFirst().orElseThrow();
        assertThat(banned.partitionResponses()).hasSize(1);
        assertThat(banned.partitionResponses().iterator().next().index()).isEqualTo(3);
        assertThat(banned.partitionResponses().iterator().next().errorCode())
                .isEqualTo(Errors.TOPIC_AUTHORIZATION_FAILED.code());
        assertThat(banned.partitionResponses().iterator().next().baseOffset()).isEqualTo(-1L);
    }

    @Test
    void shortCircuitsProduceWhenEveryTopicIsDenied() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = produceRequest("banned", 0, 1);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var result = ctx.shortCircuitResult();
        assertThat(result.body()).isInstanceOf(ProduceResponseData.class);
        var response = (ProduceResponseData) result.body();
        assertThat(response.responses()).hasSize(1);
        var topic = response.responses().iterator().next();
        assertThat(topic.name()).isEqualTo("banned");
        assertThat(topic.partitionResponses()).hasSize(2);
        assertThat(topic.partitionResponses().stream().map(p -> p.index()).toList())
                .containsExactly(0, 1);
        assertThat(topic.partitionResponses().stream().map(p -> p.errorCode()).distinct().toList())
                .containsExactly(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Test
    void deniesProduceWithAuthenticationFailedWhenThereIsNoPrincipal() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context(null);
        var request = produceRequest("orders", 0);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (ProduceResponseData) ctx.shortCircuitResult().body();
        assertThat(response.responses()).hasSize(1);
        assertThat(response.responses().iterator().next().partitionResponses().iterator().next().errorCode())
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    @Test
    void forwardsEmptyProduceRequestWithoutShortCircuiting() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = produceRequest(new ProduceRequestData());

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isFalse();
        assertThat(((ProduceRequestData) request.body()).topicData()).isEmpty();
    }

    @Test
    void deniesProduceWithEmptyResponseWhenBodyIsUndecoded() {
        var interceptor = new AuthorizationInterceptor(authorizer(), new VirtualTopicManager(Map.of()));
        var ctx = context("alice");
        var request = new TestRequest(ApiKeys.PRODUCE.id, (short) 99, null);

        interceptor.onRequest(ctx, request);

        assertThat(ctx.isShortCircuited()).isTrue();
        var response = (ProduceResponseData) ctx.shortCircuitResult().body();
        assertThat(response.responses()).isEmpty();
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
            return 9;
        }

        @Override
        public int correlationId() {
            return 42;
        }
    }
}
