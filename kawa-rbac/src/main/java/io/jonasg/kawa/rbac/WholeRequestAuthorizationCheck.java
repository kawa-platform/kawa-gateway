package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.function.Function;

/// Gates a whole-request API on a single resource: CLUSTER-resource APIs (CreateTopics,
/// DeleteTopics) on the principal's CLUSTER CREATE/DELETE permission, and group-management
/// APIs (JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat) on GROUP READ
/// for the single groupId they carry. A request that reaches this check without an authenticated principal is denied
/// rather than passed through, so skipping SASL cannot bypass RBAC.
public final class WholeRequestAuthorizationCheck implements AuthorizationCheck<Object, Object> {

    private final short apiKey;
    private final ResourceType resourceType;
    private final AclOperation operation;
    private final Function<Object, String> resourceName;
    private final RbacAuthorizer authorizer;

    public WholeRequestAuthorizationCheck(
            short apiKey, ResourceType resourceType, AclOperation operation,
            Function<Object, String> resourceName, RbacAuthorizer authorizer) {
        this.apiKey = apiKey;
        this.resourceType = resourceType;
        this.operation = operation;
        this.resourceName = resourceName;
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return apiKey;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, Object body) {
        String principal = context.principal();
        if (principal == null) {
            // No authenticated principal: the client skipped SASL. Deny rather than pass
            // through, otherwise RBAC is bypassed entirely.
            context.shortCircuit(new ShortCircuitResult(apiKey, apiVersion,
                    errorResponse(apiKey, body, Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (body == null) {
            // Undecoded body (e.g. version outside the registered range): cannot extract the
            // resource name, so deny rather than bypass.
            context.shortCircuit(new ShortCircuitResult(apiKey, apiVersion,
                    errorResponse(apiKey, body, denialError(resourceType))));
            return;
        }
        if (authorizer.isAuthorized(principal, resourceType, resourceName.apply(body), operation)) {
            return;
        }
        context.shortCircuit(new ShortCircuitResult(apiKey, apiVersion,
                errorResponse(apiKey, body, denialError(resourceType))));
    }

    private static Errors denialError(ResourceType type) {
        return switch (type) {
            case CLUSTER -> Errors.CLUSTER_AUTHORIZATION_FAILED;
            case GROUP -> Errors.GROUP_AUTHORIZATION_FAILED;
            case TOPIC -> Errors.TOPIC_AUTHORIZATION_FAILED;
            case TRANSACTIONAL_ID -> Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
            default -> throw new IllegalArgumentException("unsupported resource type " + type);
        };
    }

    private static Object errorResponse(int apiKey, Object body, Errors error) {
        if (apiKey == ApiKeys.CREATE_TOPICS.id) {
            return createTopicsError((CreateTopicsRequestData) body, error);
        }
        if (apiKey == ApiKeys.DELETE_TOPICS.id) {
            return deleteTopicsError((DeleteTopicsRequestData) body, error);
        }
        if (apiKey == ApiKeys.DESCRIBE_ACLS.id) {
            return new DescribeAclsResponseData()
                    .setErrorCode(error.code())
                    .setErrorMessage(error.message());
        }
        if (apiKey == ApiKeys.CREATE_ACLS.id) {
            return createAclsError((CreateAclsRequestData) body, error);
        }
        if (apiKey == ApiKeys.DELETE_ACLS.id) {
            return deleteAclsError((DeleteAclsRequestData) body, error);
        }
        if (apiKey == ApiKeys.DESCRIBE_LOG_DIRS.id) {
            return new DescribeLogDirsResponseData().setErrorCode(error.code());
        }
        if (apiKey == ApiKeys.JOIN_GROUP.id) {
            return new JoinGroupResponseData().setErrorCode(error.code());
        }
        if (apiKey == ApiKeys.SYNC_GROUP.id) {
            return new SyncGroupResponseData().setErrorCode(error.code());
        }
        if (apiKey == ApiKeys.HEARTBEAT.id) {
            return new HeartbeatResponseData().setErrorCode(error.code());
        }
        if (apiKey == ApiKeys.LEAVE_GROUP.id) {
            return new LeaveGroupResponseData().setErrorCode(error.code());
        }
        if (apiKey == ApiKeys.CONSUMER_GROUP_HEARTBEAT.id) {
            return new ConsumerGroupHeartbeatResponseData().setErrorCode(error.code()).setErrorMessage(error.message());
        }
        throw new IllegalArgumentException("unsupported api key " + apiKey);
    }

    private static CreateTopicsResponseData createTopicsError(CreateTopicsRequestData request, Errors error) {
        var response = new CreateTopicsResponseData();
        if (request != null) {
            for (CreateTopicsRequestData.CreatableTopic topic : request.topics()) {
                response.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
                        .setName(topic.name())
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message()));
            }
        }
        return response;
    }

    private static DeleteTopicsResponseData deleteTopicsError(DeleteTopicsRequestData request, Errors error) {
        var response = new DeleteTopicsResponseData();
        if (request != null) {
            for (String name : request.topicNames()) {
                response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                        .setName(name)
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message()));
            }
            for (DeleteTopicsRequestData.DeleteTopicState topic : request.topics()) {
                response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                        .setName(topic.name())
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message()));
            }
        }
        return response;
    }

    private static CreateAclsResponseData createAclsError(CreateAclsRequestData request, Errors error) {
        var response = new CreateAclsResponseData();
        if (request != null) {
            for (var _ : request.creations()) {
                response.results().add(new CreateAclsResponseData.AclCreationResult()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message()));
            }
        }
        return response;
    }

    private static DeleteAclsResponseData deleteAclsError(DeleteAclsRequestData request, Errors error) {
        var response = new DeleteAclsResponseData();
        if (request != null) {
            for (var _ : request.filters()) {
                response.filterResults().add(new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message()));
            }
        }
        return response;
    }
}
