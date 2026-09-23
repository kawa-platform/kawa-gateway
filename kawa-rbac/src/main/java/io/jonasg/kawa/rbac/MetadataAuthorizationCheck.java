package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.List;

/// Gates Metadata visibility on TOPIC DESCRIBE, matching real Kafka: a topic the principal
/// can't describe behaves as if it doesn't exist (UNKNOWN_TOPIC_OR_PARTITION), never
/// TOPIC_AUTHORIZATION_FAILED - unauthorized clients shouldn't be able to learn a topic exists.
///
/// Two request shapes:
/// - Specific topics named (`data.topics() != null`): denied names are stripped before
///   forwarding (avoids the broker auto-creating a topic the client has no rights to even
///   reference, if allowAutoTopicCreation is set) and a synthesized UNKNOWN_TOPIC_OR_PARTITION
///   entry is merged into the response for each.
/// - List-all (`data.topics() == null`): nothing to strip, forwarded unchanged. Filtering
///   happens entirely on the response, translating each returned (physical) topic name to its
///   virtual name via VirtualTopicManager before checking the ACL - this runs before
///   VirtualTopicInterceptor's own rename/expose-physical-topic step, so a denied entry never
///   reaches it.
public final class MetadataAuthorizationCheck implements AuthorizationCheck<MetadataRequestData, MetadataResponseData> {

    private final RbacAuthorizer authorizer;
    private final VirtualTopicManager virtualTopics;

    public MetadataAuthorizationCheck(RbacAuthorizer authorizer, VirtualTopicManager virtualTopics) {
        this.authorizer = authorizer;
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return ApiKeys.METADATA.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, MetadataRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new MetadataResponseData()));
            return;
        }
        if (data == null || data.topics() == null) {
            // List-all, or undecoded: nothing to check against a name yet, filtering happens
            // on the response.
            return;
        }
        MetadataAuthState state = null;
        var denied = new ArrayList<MetadataRequestData.MetadataRequestTopic>();
        for (MetadataRequestData.MetadataRequestTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.DESCRIBE)) {
                denied.add(topic);
            }
        }
        for (MetadataRequestData.MetadataRequestTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(MetadataAuthState.class);
                if (state == null) {
                    state = new MetadataAuthState();
                    context.state(MetadataAuthState.class, state);
                }
            }
            state.recordDenied(topic.name());
        }
        // Same guard as Produce/Fetch: only short-circuit if something was actually denied.
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    metadataDenialResponse(state.deniedTopics())));
        }
    }

    @Override
    public void onResponse(GatewayContext context, MetadataResponseData data) {
        String principal = context.principal();
        if (principal == null) {
            return; // request-side already denied; nothing more to do
        }
        // List-all filtering first: remove any topic the principal can't describe, translating
        // the broker's physical name to virtual first since this runs before
        // VirtualTopicInterceptor.onResponse. This must run BEFORE appending the synthesized
        // denial entries below - those carry UNKNOWN_TOPIC_OR_PARTITION and a name the principal
        // is by definition denied DESCRIBE on, so removeIf would immediately delete them again.
        data.topics().removeIf(topic -> {
            String virtual = virtualTopics.toVirtual(topic.name());
            return !authorizer.isAuthorized(principal, ResourceType.TOPIC, virtual, AclOperation.DESCRIBE);
        });
        // Then append the synthesized denial entries for the specific-topics case.
        MetadataAuthState state = context.state(MetadataAuthState.class);
        if (state != null && !state.deniedTopics().isEmpty()) {
            for (String name : state.deniedTopics()) {
                data.topics().add(new MetadataResponseData.MetadataResponseTopic()
                        .setName(name)
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
            }
        }
    }

    private static MetadataResponseData metadataDenialResponse(List<String> deniedTopics) {
        var response = new MetadataResponseData();
        for (String name : deniedTopics) {
            response.topics().add(new MetadataResponseData.MetadataResponseTopic()
                    .setName(name)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
        }
        return response;
    }
}
