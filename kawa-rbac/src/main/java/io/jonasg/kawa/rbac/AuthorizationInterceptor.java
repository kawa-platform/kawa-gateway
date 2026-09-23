package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;

import java.util.List;
import java.util.Set;

/// Enforces RBAC on the client-to-broker path. Dispatches to a per-apiKey
/// [AuthorizationCheck] via [AuthorizationCheckRegistry]; see that registry's members for what
/// each apiKey actually enforces.
public final class AuthorizationInterceptor implements Interceptor {

    private static final String CLUSTER_RESOURCE = "kafka-cluster";

    private final AuthorizationCheckRegistry registry;

    public AuthorizationInterceptor(RbacAuthorizer authorizer, VirtualTopicManager virtualTopics) {
        this.registry = new AuthorizationCheckRegistry(List.of(
                new WholeRequestAuthorizationCheck(ApiKeys.CREATE_TOPICS.id,
                        ResourceType.CLUSTER, AclOperation.CREATE, _ -> CLUSTER_RESOURCE, authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.DELETE_TOPICS.id,
                        ResourceType.CLUSTER, AclOperation.DELETE, _ -> CLUSTER_RESOURCE, authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.DESCRIBE_ACLS.id,
                        ResourceType.CLUSTER, AclOperation.DESCRIBE, _ -> CLUSTER_RESOURCE, authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.CREATE_ACLS.id,
                        ResourceType.CLUSTER, AclOperation.ALTER, _ -> CLUSTER_RESOURCE, authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.DELETE_ACLS.id,
                        ResourceType.CLUSTER, AclOperation.ALTER, _ -> CLUSTER_RESOURCE, authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.DESCRIBE_LOG_DIRS.id,
                        ResourceType.CLUSTER, AclOperation.DESCRIBE, _ -> CLUSTER_RESOURCE, authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.JOIN_GROUP.id,
                        ResourceType.GROUP, AclOperation.READ,
                        body -> ((JoinGroupRequestData) body).groupId(), authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.SYNC_GROUP.id,
                        ResourceType.GROUP, AclOperation.READ,
                        body -> ((SyncGroupRequestData) body).groupId(), authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.HEARTBEAT.id,
                        ResourceType.GROUP, AclOperation.READ,
                        body -> ((HeartbeatRequestData) body).groupId(), authorizer),
                new WholeRequestAuthorizationCheck(ApiKeys.LEAVE_GROUP.id,
                        ResourceType.GROUP, AclOperation.READ,
                        body -> ((LeaveGroupRequestData) body).groupId(), authorizer),
                new ProduceAuthorizationCheck(authorizer),
                new FetchAuthorizationCheck(authorizer),
                new ListOffsetsAuthorizationCheck(authorizer),
                new DeleteRecordsAuthorizationCheck(authorizer),
                new OffsetForLeaderEpochAuthorizationCheck(authorizer),
                new OffsetCommitAuthorizationCheck(authorizer),
                new OffsetFetchAuthorizationCheck(authorizer),
                new DescribeConfigsAuthorizationCheck(authorizer),
                new AlterConfigsAuthorizationCheck<>(
                        ApiKeys.ALTER_CONFIGS.id, authorizer,
                        AlterConfigsResponseData::new,
                        AlterConfigsRequestData::resources,
                        AlterConfigsRequestData.AlterConfigsResource::resourceType,
                        AlterConfigsRequestData.AlterConfigsResource::resourceName,
                        AlterConfigsResponseData::responses,
                        (name, errorCode) -> new AlterConfigsResponseData.AlterConfigsResourceResponse()
                                .setResourceType(ConfigResource.Type.TOPIC.id())
                                .setResourceName(name)
                                .setErrorCode(errorCode)),
                new AlterConfigsAuthorizationCheck<>(
                        ApiKeys.INCREMENTAL_ALTER_CONFIGS.id, authorizer,
                        IncrementalAlterConfigsResponseData::new,
                        IncrementalAlterConfigsRequestData::resources,
                        IncrementalAlterConfigsRequestData.AlterConfigsResource::resourceType,
                        IncrementalAlterConfigsRequestData.AlterConfigsResource::resourceName,
                        IncrementalAlterConfigsResponseData::responses,
                        (name, errorCode) -> new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                                .setResourceType(ConfigResource.Type.TOPIC.id())
                                .setResourceName(name)
                                .setErrorCode(errorCode)),
                new MetadataAuthorizationCheck(authorizer, virtualTopics),
                new DescribeTopicPartitionsAuthorizationCheck(authorizer),
                new ListGroupsAuthorizationCheck(authorizer),
                new DescribeGroupsAuthorizationCheck(authorizer),
                new FindCoordinatorAuthorizationCheck(authorizer),
                new CreatePartitionsAuthorizationCheck(authorizer),
                new OffsetDeleteAuthorizationCheck(authorizer),
                new DescribeTransactionsAuthorizationCheck(authorizer),
                new AddPartitionsToTxnAuthorizationCheck(authorizer),
                new TxnOffsetCommitAuthorizationCheck(authorizer)));
    }

    @Override
    public void onRequest(GatewayContext context, Request request) {
        short apiKey = (short) request.apiKey();
        if (!registry.hasApiKey(apiKey)) {
            return;
        }
        registry.onRequest(context, apiKey, request.apiVersion(), request.body());
    }

    @Override
    public void onResponse(GatewayContext context, Response response) {
        registry.onResponse(context, (short) response.apiKey(), response.body());
    }

    /// The api keys this interceptor gates. Exposed so the RBAC surface can be audited against
    /// the protocol registry to catch gaps (registered-but-ungated or gated-but-unregistered).
    public Set<Short> gatedApiKeys() {
        return registry.apiKeys();
    }
}
