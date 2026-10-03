package io.jonasg.kawa.governance;

import io.jonasg.kawa.config.GovernanceRuleConfig.Operation;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Map;

/// A request as governance sees it: who asks, through which service, for which resource.
///
/// [resource] holds the fields bound under the resource's CEL variable: for `TOPIC` `name`,
/// `partitions`, `replicationFactor` and `configs` (physical) or `name` and `physicalTopic`
/// (virtual); for `GROUP` and `TRANSACTIONAL_ID` `id`. `topic.virtual` is bound from [virtual].
///
/// On [Operation#ALTER] a topic is described as it will be after the change: its current
/// partitions, replication factor and configs (overrides) with the change applied. On
/// [Operation#DELETE] it is described as it is.
///
/// @param resourceType `TOPIC`, `GROUP` or `TRANSACTIONAL_ID`
/// @param operation    create, alter or delete; only topic rules running on it apply, other resource types have no operations
/// @param virtual      topics only: whether the topic is virtual
/// @param resource     the resource's fields
/// @param principal    the requesting principal
/// @param service      the service the request came through
public record GovernanceRequest(
        ResourceType resourceType,
        Operation operation,
        boolean virtual,
        Map<String, Object> resource,
        String principal,
        String service
) {

    public GovernanceRequest {
        if (resourceType == null) {
            throw new IllegalArgumentException("resourceType must not be null");
        }
        operation = operation == null ? Operation.CREATE : operation;
        resource = resource == null ? Map.of() : Map.copyOf(resource);
        principal = principal == null ? "" : principal;
        service = service == null ? "" : service;
    }

    /// A physical topic creation, as [GovernancePolicy#evaluate] receives it.
    public static GovernanceRequest topic(String principal, String service, TopicSpec topic) {
        return new GovernanceRequest(ResourceType.TOPIC, Operation.CREATE, false, Map.of(
                "name", topic.name(),
                "partitions", topic.partitions(),
                "replicationFactor", topic.replicationFactor(),
                "configs", topic.configs()), principal, service);
    }

    /// A change to a physical topic, described as the topic will be once it is applied.
    public static GovernanceRequest topicAlter(String principal, String service, TopicSpec after) {
        return new GovernanceRequest(ResourceType.TOPIC, Operation.ALTER, false, Map.of(
                "name", after.name(),
                "partitions", after.partitions(),
                "replicationFactor", after.replicationFactor(),
                "configs", after.configs()), principal, service);
    }

    /// A physical topic being deleted, as it is now.
    public static GovernanceRequest topicDelete(String principal, String service, TopicSpec current) {
        return new GovernanceRequest(ResourceType.TOPIC, Operation.DELETE, false, Map.of(
                "name", current.name(),
                "partitions", current.partitions(),
                "replicationFactor", current.replicationFactor(),
                "configs", current.configs()), principal, service);
    }

    /// A virtual topic being created, changed or deleted: its client-facing name and the physical
    /// topic it maps onto.
    public static GovernanceRequest virtualTopic(String principal, String service, Operation operation, String name, String physicalTopic) {
        return new GovernanceRequest(ResourceType.TOPIC, operation, true, Map.of(
                "name", name,
                "physicalTopic", physicalTopic), principal, service);
    }

    /// A consumer group a client joins or commits offsets for.
    public static GovernanceRequest group(String principal, String service, String id) {
        return new GovernanceRequest(ResourceType.GROUP, Operation.CREATE, false, Map.of("id", id == null ? "" : id), principal, service);
    }

    /// A transactional id a producer initialises.
    public static GovernanceRequest transaction(String principal, String service, String id) {
        return new GovernanceRequest(ResourceType.TRANSACTIONAL_ID, Operation.CREATE, false, Map.of("id", id == null ? "" : id),
                principal, service);
    }

    /// The resource's name or id, whichever it has.
    public String resourceName() {
        Object name = resource.getOrDefault("name", resource.get("id"));
        return name == null ? "" : name.toString();
    }
}
