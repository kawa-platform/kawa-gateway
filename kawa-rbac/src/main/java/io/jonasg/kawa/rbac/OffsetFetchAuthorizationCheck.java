package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/// Gates OffsetFetch on two resources: a whole-request GROUP DESCRIBE check on the request's
/// groupId, and a per-topic TOPIC DESCRIBE check on each named topic. If the group gate fails
/// (or there is no authenticated principal) the whole request is short-circuited with a
/// GROUP_AUTHORIZATION_FAILED (or SASL_AUTHENTICATION_FAILED) top-level error - nothing can
/// proceed without group access, so the per-topic checks are skipped entirely. Otherwise
/// authorized topics are forwarded, denied topics are stripped from the request and remembered
/// so the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED entry per denied topic.
///
/// Scoped to the old single-group shape only (`groupId()` + `topics()`). The newer batched
/// `.groups()` shape is not decoded by kawa's protocol layer, so it is not gated here. When
/// `topics()` is null (fetch-all offsets for the group) the request is forwarded unchanged and the
/// response is filtered instead, as a broker does: a topic the principal may not describe is left
/// out. This runs before VirtualTopicInterceptor renames the response, so each returned (physical)
/// topic is checked under its virtual names: the entry is removed when the principal may describe
/// none of them, and otherwise the ones it may not describe are hidden so VirtualTopicInterceptor
/// leaves them out.
public final class OffsetFetchAuthorizationCheck implements AuthorizationCheck<OffsetFetchRequestData, OffsetFetchResponseData> {

    private final RbacAuthorizer authorizer;
    private final VirtualTopicManager virtualTopics;

    public OffsetFetchAuthorizationCheck(RbacAuthorizer authorizer, VirtualTopicManager virtualTopics) {
        this.authorizer = authorizer;
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return ApiKeys.OFFSET_FETCH.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, OffsetFetchRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    groupDenialResponse(Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new OffsetFetchResponseData()));
            return;
        }
        // Gate 1: GROUP DESCRIBE on the groupId. All-or-nothing - if denied, nothing can proceed.
        if (!authorizer.isAuthorized(principal, ResourceType.GROUP, data.groupId(), AclOperation.DESCRIBE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    groupDenialResponse(Errors.GROUP_AUTHORIZATION_FAILED)));
            return;
        }
        // Gate 2: per-topic TOPIC DESCRIBE. topics() == null means "fetch all offsets for this
        // group" - forward unchanged, the response is filtered instead.
        if (data.topics() == null) {
            stateOf(context).recordAllTopics();
            return;
        }
        OffsetFetchAuthState state = null;
        var denied = new ArrayList<OffsetFetchRequestData.OffsetFetchRequestTopic>();
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.DESCRIBE)) {
                denied.add(topic);
            }
        }
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = stateOf(context);
            }
            state.recordDenied(topic.name());
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    topicDenialResponse(state.deniedTopics(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, OffsetFetchResponseData data) {
        OffsetFetchAuthState state = context.state(OffsetFetchAuthState.class);
        if (state == null) {
            return;
        }
        if (state.allTopics()) {
            filterUndescribable(context, data);
        }
        if (!state.deniedTopics().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static OffsetFetchAuthState stateOf(GatewayContext context) {
        OffsetFetchAuthState state = context.state(OffsetFetchAuthState.class);
        if (state == null) {
            state = new OffsetFetchAuthState();
            context.state(OffsetFetchAuthState.class, state);
        }
        return state;
    }

    /// Leaves out of an all-topics response every topic the principal may not describe, under the names the
    /// client sees (see the class comment).
    private void filterUndescribable(GatewayContext context, OffsetFetchResponseData data) {
        String principal = context.principal();
        data.topics().removeIf(topic -> {
            List<String> virtuals = virtualTopics.toVirtual(topic.name());
            List<String> denied = virtuals.stream()
                    .filter(virtual -> !authorizer.isAuthorized(principal, ResourceType.TOPIC, virtual, AclOperation.DESCRIBE))
                    .toList();
            if (denied.size() == virtuals.size()) {
                return true;
            }
            if (!denied.isEmpty()) {
                var virtualTopicState = VirtualTopicState.from(context);
                denied.forEach(virtualTopicState::hide);
            }
            return false;
        });
    }

    private static void mergeDenials(OffsetFetchResponseData data, OffsetFetchAuthState state) {
        for (String topic : state.deniedTopics()) {
            data.topics().add(topicDenialResponse(topic, Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static OffsetFetchResponseData groupDenialResponse(Errors error) {
        return new OffsetFetchResponseData().setErrorCode(error.code());
    }

    private static OffsetFetchResponseData topicDenialResponse(Collection<String> deniedTopics, Errors error) {
        var response = new OffsetFetchResponseData();
        for (String topic : deniedTopics) {
            response.topics().add(topicDenialResponse(topic, error));
        }
        return response;
    }

    private static OffsetFetchResponseData.OffsetFetchResponseTopic topicDenialResponse(String topic, Errors error) {
        var topicResponse = new OffsetFetchResponseData.OffsetFetchResponseTopic().setName(topic);
        var partitionResponses = new ArrayList<OffsetFetchResponseData.OffsetFetchResponsePartition>();
        partitionResponses.add(new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(-1)
                .setErrorCode(error.code()));
        topicResponse.setPartitions(partitionResponses);
        return topicResponse;
    }
}
