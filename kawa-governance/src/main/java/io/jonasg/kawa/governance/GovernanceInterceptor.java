package io.jonasg.kawa.governance;

import io.jonasg.kawa.config.GovernanceRuleConfig.Operation;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.core.ShortCircuitResult;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/// Enforces governance on the Kafka protocol, answering each refusal with the closest standard
/// Kafka error:
///
/// | Request | Judged as | Refused with |
/// |---|---|---|
/// | CreateTopics | topic `CREATE` | `POLICY_VIOLATION` per topic |
/// | AlterConfigs, IncrementalAlterConfigs (TOPIC resources) | topic `ALTER` | `POLICY_VIOLATION` per resource |
/// | CreatePartitions | topic `ALTER` | `POLICY_VIOLATION` per topic |
/// | DeleteTopics | topic `DELETE` | `POLICY_VIOLATION` per topic |
/// | JoinGroup, ConsumerGroupHeartbeat | group | `INVALID_GROUP_ID` |
/// | OffsetCommit, TxnOffsetCommit | group | `INVALID_GROUP_ID` per partition |
/// | InitProducerId with a transactional id | transactional id | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` |
///
/// A change (`ALTER`) is judged against the topic as it will be once applied: [#prepare]
/// reads its current partitions, replication factor and configs (overrides) from the broker
/// through a [TopicDescriber], and the change is applied on top. AlterConfigs replaces every
/// override; IncrementalAlterConfigs sets, deletes, appends to or subtracts from them;
/// CreatePartitions sets the partition count. A delete (`DELETE`) is judged against the
/// topic as it is, read the same way. A topic the broker does not know is forwarded
/// for the broker to answer. When the state cannot be read, the change is refused. Nothing is
/// read while no rule runs on `ALTER`. A change through a virtual topic name is judged as a
/// change to its physical topic, which is what it changes; creating a topic under a virtual
/// topic's name is left to the virtual topic interceptor, which refuses it. Virtual topics
/// themselves are created and changed through the admin API, which judges them there.
///
/// The governance message, e.g. `[toys > colour] toys must be red`, goes in the response's
/// error message where the API has one (CreateTopics, the alter configs APIs,
/// CreatePartitions, DeleteTopics, ConsumerGroupHeartbeat). Every refusal is logged with it.
///
/// Requests with per-entry results (CreateTopics, the alter configs APIs, CreatePartitions,
/// DeleteTopics, the offset commits) are never short-circuited: refused entries are stripped
/// so the broker never sees them and added back to the response, so other interceptors (RBAC)
/// still add their own denials. A request whose every entry was refused is forwarded empty. Whole-request APIs (JoinGroup,
/// ConsumerGroupHeartbeat, InitProducerId) are answered by the gateway. A rule that fails to
/// evaluate refuses (fail-closed).
///
/// Runs after RBAC: a request RBAC refused never reaches governance.
public final class GovernanceInterceptor implements Interceptor {

    /// The `service` CEL binding for requests that arrive on the Kafka listener.
    public static final String SERVICE = "kafka";

    private static final short CREATE_TOPICS = ApiKeys.CREATE_TOPICS.id;
    private static final short ALTER_CONFIGS = ApiKeys.ALTER_CONFIGS.id;
    private static final short INCREMENTAL_ALTER_CONFIGS = ApiKeys.INCREMENTAL_ALTER_CONFIGS.id;
    private static final short CREATE_PARTITIONS = ApiKeys.CREATE_PARTITIONS.id;
    private static final short DELETE_TOPICS = ApiKeys.DELETE_TOPICS.id;
    private static final byte SET = 0;
    private static final byte DELETE = 1;
    private static final byte APPEND = 2;
    private static final byte SUBTRACT = 3;
    private static final Set<Short> GOVERNED = Set.of(
            CREATE_TOPICS, ALTER_CONFIGS, INCREMENTAL_ALTER_CONFIGS, CREATE_PARTITIONS, DELETE_TOPICS,
            ApiKeys.JOIN_GROUP.id, ApiKeys.CONSUMER_GROUP_HEARTBEAT.id,
            ApiKeys.OFFSET_COMMIT.id, ApiKeys.TXN_OFFSET_COMMIT.id,
            ApiKeys.INIT_PRODUCER_ID.id);
    private static final Logger LOG = LoggerFactory.getLogger(GovernanceInterceptor.class);

    private final Supplier<GovernancePolicy> policy;
    private final TopicDescriber topics;
    private final UnaryOperator<String> physicalName;

    /// @param policy       the live policy; a supplier so a gateway reload is picked up per request
    /// @param topics       reads a topic's current state, to judge a change against
    /// @param physicalName maps a client-facing topic name to the physical topic the broker
    ///                     sees: a change through a virtual topic name changes its physical topic
    public GovernanceInterceptor(Supplier<GovernancePolicy> policy, TopicDescriber topics, UnaryOperator<String> physicalName) {
        this.policy = policy;
        this.topics = topics;
        this.physicalName = physicalName;
    }

    /// Without virtual topics: every name is a physical topic.
    public GovernanceInterceptor(GovernancePolicy policy, TopicDescriber topics) {
        this(() -> policy, topics, UnaryOperator.identity());
    }

    /// Reads the current state of the topics a change touches, when a rule runs on `ALTER`.
    @Override
    public CompletionStage<?> prepare(GatewayContext context, Request request) {
        Set<String> names = new LinkedHashSet<>();
        Operation operation = Operation.ALTER;
        switch (request.body()) {
            case AlterConfigsRequestData data -> data.resources().stream()
                    .filter(r -> r.resourceType() == ConfigResource.Type.TOPIC.id())
                    .forEach(r -> names.add(physicalName.apply(r.resourceName())));
            case IncrementalAlterConfigsRequestData data -> data.resources().stream()
                    .filter(r -> r.resourceType() == ConfigResource.Type.TOPIC.id())
                    .forEach(r -> names.add(physicalName.apply(r.resourceName())));
            case CreatePartitionsRequestData data -> data.topics().forEach(t -> names.add(physicalName.apply(t.name())));
            case DeleteTopicsRequestData data -> {
                operation = Operation.DELETE;
                deletedNames(data).stream().filter(this::isPhysicalName).forEach(names::add);
            }
            case null, default -> {
            }
        }
        if (names.isEmpty() || !policy.get().hasPhysicalTopicRules(operation)) {
            return null;
        }
        CompletionStage<Map<String, TopicState>> described;
        try {
            described = topics.describe(names);
        } catch (RuntimeException e) {
            context.state(CurrentTopics.class, new CurrentTopics(Map.of(), e));
            return null;
        }
        return described.handle((states, error) -> {
            context.state(CurrentTopics.class, new CurrentTopics(states == null ? Map.of() : states, error));
            return null;
        });
    }

    @Override
    public boolean appliesToRequest(Request request) {
        return isGoverned((short) request.apiKey());
    }

    @Override
    public boolean appliesToResponse(Response response) {
        return isGoverned((short) response.apiKey());
    }

    private static boolean isGoverned(short apiKey) {
        return GOVERNED.contains(apiKey);
    }

    @Override
    public void onRequest(GatewayContext context, Request request) {
        String principal = context.principal() == null ? "" : context.principal();
        Map<String, String> refused = switch (request.body()) {
            case CreateTopicsRequestData data -> createTopics(principal, data);
            case AlterConfigsRequestData data -> alterConfigs(principal, context, data);
            case IncrementalAlterConfigsRequestData data -> incrementalAlterConfigs(principal, context, data);
            case CreatePartitionsRequestData data -> createPartitions(principal, context, data);
            case DeleteTopicsRequestData data -> deleteTopics(principal, context, data);
            case JoinGroupRequestData data -> {
                refusal("JoinGroup", GovernanceRequest.group(principal, SERVICE, data.groupId())).ifPresent(_ ->
                        context.shortCircuit(new ShortCircuitResult((short) request.apiKey(), request.apiVersion(),
                                new JoinGroupResponseData().setErrorCode(Errors.INVALID_GROUP_ID.code()))));
                yield Map.of();
            }
            case ConsumerGroupHeartbeatRequestData data when data.memberEpoch() >= 0 -> { // a member may always leave
                refusal("ConsumerGroupHeartbeat", GovernanceRequest.group(principal, SERVICE, data.groupId())).ifPresent(message ->
                        context.shortCircuit(new ShortCircuitResult((short) request.apiKey(), request.apiVersion(),
                                new ConsumerGroupHeartbeatResponseData()
                                        .setErrorCode(Errors.INVALID_GROUP_ID.code()).setErrorMessage(message))));
                yield Map.of();
            }
            case InitProducerIdRequestData data when data.transactionalId() != null -> {
                refusal("InitProducerId", GovernanceRequest.transaction(principal, SERVICE, data.transactionalId())).ifPresent(_ ->
                        context.shortCircuit(new ShortCircuitResult((short) request.apiKey(), request.apiVersion(),
                                new InitProducerIdResponseData()
                                        .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code())
                                        .setProducerId(-1L).setProducerEpoch((short) -1))));
                yield Map.of();
            }
            case OffsetCommitRequestData data -> {
                refusal("OffsetCommit", GovernanceRequest.group(principal, SERVICE, data.groupId())).ifPresent(_ -> {
                    Map<String, List<Integer>> partitions = new LinkedHashMap<>();
                    data.topics().forEach(t -> partitions.put(t.name(),
                            t.partitions().stream().map(OffsetCommitRequestData.OffsetCommitRequestPartition::partitionIndex).toList()));
                    data.topics().clear();
                    context.state(CommitRefusal.class, new CommitRefusal(partitions));
                });
                yield Map.of();
            }
            case TxnOffsetCommitRequestData data -> {
                refusal("TxnOffsetCommit", GovernanceRequest.group(principal, SERVICE, data.groupId())).ifPresent(_ -> {
                    Map<String, List<Integer>> partitions = new LinkedHashMap<>();
                    data.topics().forEach(t -> partitions.put(t.name(),
                            t.partitions().stream().map(TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition::partitionIndex).toList()));
                    data.topics().clear();
                    context.state(CommitRefusal.class, new CommitRefusal(partitions));
                });
                yield Map.of();
            }
            case null, default -> Map.of();
        };
        if (!refused.isEmpty()) {
            context.state(Refusals.class, new Refusals(refused));
        }
    }

    @Override
    public void onResponse(GatewayContext context, Response response) {
        CommitRefusal commit = context.state(CommitRefusal.class);
        if (commit != null) {
            commitRefusal(response.body(), commit);
        }
        Refusals refusals = context.state(Refusals.class);
        if (refusals == null) {
            return;
        }
        short code = Errors.POLICY_VIOLATION.code();
        switch (response.body()) {
            case CreateTopicsResponseData data -> refusals.byName().forEach((name, message) ->
                    data.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
                            .setName(name).setErrorCode(code).setErrorMessage(message)));
            case AlterConfigsResponseData data -> refusals.byName().forEach((name, message) ->
                    data.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                            .setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(name)
                            .setErrorCode(code).setErrorMessage(message)));
            case IncrementalAlterConfigsResponseData data -> refusals.byName().forEach((name, message) ->
                    data.responses().add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                            .setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(name)
                            .setErrorCode(code).setErrorMessage(message)));
            case CreatePartitionsResponseData data -> refusals.byName().forEach((name, message) ->
                    data.results().add(new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                            .setName(name).setErrorCode(code).setErrorMessage(message)));
            case DeleteTopicsResponseData data -> {
                refusals.byName().forEach((name, message) -> data.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                        .setName(name).setErrorCode(code).setErrorMessage(message)));
                refusals.byTopicId().forEach((id, message) -> data.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                        .setName(null).setTopicId(id).setErrorCode(code).setErrorMessage(message)));
            }
            case null, default -> {
            }
        }
    }

    /// Answers every partition of a refused offset commit with `INVALID_GROUP_ID`; neither
    /// response carries an error message.
    private static void commitRefusal(Object body, CommitRefusal refusal) {
        short code = Errors.INVALID_GROUP_ID.code();
        switch (body) {
            case OffsetCommitResponseData data -> refusal.partitionsByTopic().forEach((topic, partitions) ->
                    data.topics().add(new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(topic)
                            .setPartitions(new ArrayList<>(partitions.stream()
                                    .map(p -> new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                            .setPartitionIndex(p).setErrorCode(code))
                                    .toList()))));
            case TxnOffsetCommitResponseData data -> refusal.partitionsByTopic().forEach((topic, partitions) ->
                    data.topics().add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName(topic)
                            .setPartitions(new ArrayList<>(partitions.stream()
                                    .map(p -> new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                            .setPartitionIndex(p).setErrorCode(code))
                                    .toList()))));
            case null, default -> {
            }
        }
    }

    private Map<String, String> createTopics(String principal, CreateTopicsRequestData data) {
        Map<String, String> refused = new LinkedHashMap<>();
        var denied = new ArrayList<CreateTopicsRequestData.CreatableTopic>();
        for (var topic : data.topics()) {
            if (!isPhysicalName(topic.name())) {
                continue; // a virtual topic's name: the virtual topic interceptor refuses it
            }
            Map<String, String> configs = new HashMap<>();
            topic.configs().forEach(c -> configs.put(c.name(), c.value()));
            int partitions = topic.numPartitions();
            int replicationFactor = topic.replicationFactor();
            if (!topic.assignments().isEmpty()) {
                partitions = topic.assignments().size();
                replicationFactor = topic.assignments().iterator().next().brokerIds().size();
            }
            var spec = new TopicSpec(topic.name(), partitions, replicationFactor, configs);
            refusal("CreateTopics", GovernanceRequest.topic(principal, SERVICE, spec)).ifPresent(message -> {
                refused.put(topic.name(), message);
                denied.add(topic);
            });
        }
        denied.forEach(data.topics()::remove);
        return refused;
    }

    /// AlterConfigs replaces every override on the topic with the ones it sends.
    private Map<String, String> alterConfigs(String principal, GatewayContext context, AlterConfigsRequestData data) {
        Map<String, String> refused = new LinkedHashMap<>();
        if (!policy.get().hasPhysicalTopicRules(Operation.ALTER)) {
            return refused;
        }
        CurrentTopics current = context.state(CurrentTopics.class);
        var denied = new ArrayList<AlterConfigsRequestData.AlterConfigsResource>();
        for (var resource : data.resources()) {
            if (resource.resourceType() != ConfigResource.Type.TOPIC.id()) {
                continue;
            }
            alterRefusal("AlterConfigs", principal, current, resource.resourceName(), now -> {
                Map<String, String> configs = new HashMap<>();
                resource.configs().forEach(c -> {
                    if (c.value() != null) {
                        configs.put(c.name(), c.value());
                    }
                });
                return new TopicState(now.partitions(), now.replicationFactor(), configs);
            }).ifPresent(message -> {
                refused.put(resource.resourceName(), message);
                denied.add(resource);
            });
        }
        denied.forEach(data.resources()::remove);
        return refused;
    }

    private Map<String, String> incrementalAlterConfigs(String principal, GatewayContext context, IncrementalAlterConfigsRequestData data) {
        Map<String, String> refused = new LinkedHashMap<>();
        if (!policy.get().hasPhysicalTopicRules(Operation.ALTER)) {
            return refused;
        }
        CurrentTopics current = context.state(CurrentTopics.class);
        var denied = new ArrayList<IncrementalAlterConfigsRequestData.AlterConfigsResource>();
        for (var resource : data.resources()) {
            if (resource.resourceType() != ConfigResource.Type.TOPIC.id()) {
                continue;
            }
            alterRefusal("IncrementalAlterConfigs", principal, current, resource.resourceName(), now ->
                    new TopicState(now.partitions(), now.replicationFactor(),
                            applyIncremental(now.configs(), resource.configs()))).ifPresent(message -> {
                refused.put(resource.resourceName(), message);
                denied.add(resource);
            });
        }
        denied.forEach(data.resources()::remove);
        return refused;
    }

    /// CreatePartitions changes only the partition count.
    private Map<String, String> createPartitions(String principal, GatewayContext context, CreatePartitionsRequestData data) {
        Map<String, String> refused = new LinkedHashMap<>();
        if (!policy.get().hasPhysicalTopicRules(Operation.ALTER)) {
            return refused;
        }
        CurrentTopics current = context.state(CurrentTopics.class);
        var denied = new ArrayList<CreatePartitionsRequestData.CreatePartitionsTopic>();
        for (var topic : data.topics()) {
            alterRefusal("CreatePartitions", principal, current, topic.name(), now ->
                    new TopicState(topic.count(), now.replicationFactor(), now.configs())).ifPresent(message -> {
                refused.put(topic.name(), message);
                denied.add(topic);
            });
        }
        denied.forEach(data.topics()::remove);
        return refused;
    }

    /// DeleteTopics, judged against each topic as it is. A topic named by id only cannot be
    /// judged, so it is refused while a rule runs on `DELETE`; a virtual topic's name is left to
    /// the virtual topic interceptor, which refuses it.
    private Map<String, String> deleteTopics(String principal, GatewayContext context, DeleteTopicsRequestData data) {
        Map<String, String> refused = new LinkedHashMap<>();
        if (!policy.get().hasPhysicalTopicRules(Operation.DELETE)) {
            return refused;
        }
        CurrentTopics current = context.state(CurrentTopics.class);
        var deniedNames = new ArrayList<String>();
        for (String name : data.topicNames()) {
            if (isPhysicalName(name)) {
                changeRefusal("DeleteTopics", Operation.DELETE, principal, current, name, UnaryOperator.identity()).ifPresent(message -> {
                    refused.put(name, message);
                    deniedNames.add(name);
                });
            }
        }
        deniedNames.forEach(data.topicNames()::remove);
        Map<Uuid, String> refusedIds = new LinkedHashMap<>();
        var denied = new ArrayList<DeleteTopicsRequestData.DeleteTopicState>();
        for (var topic : data.topics()) {
            if (topic.name() == null) {
                String message = "governance cannot judge a delete by topic id; delete the topic by name";
                LOG.info("governance refused DeleteTopics for topic id {} by principal '{}': {}", topic.topicId(), principal, message);
                refusedIds.put(topic.topicId(), message);
                denied.add(topic);
            } else if (isPhysicalName(topic.name())) {
                changeRefusal("DeleteTopics", Operation.DELETE, principal, current, topic.name(), UnaryOperator.identity())
                        .ifPresent(message -> {
                            refused.put(topic.name(), message);
                            denied.add(topic);
                        });
            }
        }
        denied.forEach(data.topics()::remove);
        if (!refusedIds.isEmpty()) {
            context.state(Refusals.class, new Refusals(refused, refusedIds));
            return Map.of();
        }
        return refused;
    }

    private static List<String> deletedNames(DeleteTopicsRequestData data) {
        List<String> names = new ArrayList<>(data.topicNames());
        data.topics().stream().map(DeleteTopicsRequestData.DeleteTopicState::name).filter(Objects::nonNull).forEach(names::add);
        return names;
    }

    /// Whether `name` is a physical topic's name rather than a virtual topic's.
    private boolean isPhysicalName(String name) {
        return physicalName.apply(name).equals(name);
    }

    /// Judges a change to `name` as its physical topic will be after it: a change through a
    /// virtual topic name changes the physical topic. A topic the broker does not know is let
    /// through for the broker to answer; when the state could not be read the change is
    /// refused.
    private Optional<String> alterRefusal(
            String api, String principal, CurrentTopics current, String name, UnaryOperator<TopicState> after) {
        return changeRefusal(api, Operation.ALTER, principal, current, name, after);
    }

    /// Judges `operation` on `name`'s physical topic: on `ALTER` as it will be after the change,
    /// on `DELETE` as it is.
    private Optional<String> changeRefusal(
            String api, Operation operation, String principal, CurrentTopics current, String name, UnaryOperator<TopicState> after) {
        if (name == null || name.isBlank()) {
            return Optional.empty(); // no topic to judge; the broker refuses it
        }
        if (current == null || current.error() != null) {
            String reason = current == null ? "it was not read" : rootMessage(current.error());
            String message = "governance could not read the current state of topic '" + name + "': " + reason;
            LOG.warn("governance refused {} for topic '{}' by principal '{}': {}", api, name, principal, message);
            return Optional.of(message);
        }
        String physical = physicalName.apply(name);
        TopicState now = current.byName().get(physical);
        if (now == null) {
            return Optional.empty();
        }
        TopicState changed = after.apply(now);
        var spec = new TopicSpec(physical, changed.partitions(), changed.replicationFactor(), changed.configs());
        return refusal(api, operation == Operation.DELETE
                ? GovernanceRequest.topicDelete(principal, SERVICE, spec)
                : GovernanceRequest.topicAlter(principal, SERVICE, spec));
    }

    /// The topic's overrides once the operations are applied, in order. `APPEND` and
    /// `SUBTRACT` work on comma-separated lists; appending to a config the topic does not set
    /// starts from an empty list, not the broker default.
    static Map<String, String> applyIncremental(
            Map<String, String> current, Collection<IncrementalAlterConfigsRequestData.AlterableConfig> operations) {
        Map<String, String> configs = new HashMap<>(current);
        for (var op : operations) {
            switch (op.configOperation()) {
                case SET -> {
                    if (op.value() == null) {
                        configs.remove(op.name());
                    } else {
                        configs.put(op.name(), op.value());
                    }
                }
                case DELETE -> configs.remove(op.name());
                case APPEND -> {
                    List<String> items = new ArrayList<>(items(configs.get(op.name())));
                    items(op.value()).stream().filter(item -> !items.contains(item)).forEach(items::add);
                    configs.put(op.name(), String.join(",", items));
                }
                case SUBTRACT -> {
                    if (configs.containsKey(op.name())) {
                        List<String> items = new ArrayList<>(items(configs.get(op.name())));
                        items.removeAll(items(op.value()));
                        configs.put(op.name(), String.join(",", items));
                    }
                }
                default -> {
                }
            }
        }
        return configs;
    }

    private static List<String> items(String list) {
        if (list == null || list.isBlank()) {
            return List.of();
        }
        return Arrays.stream(list.split(",")).map(String::trim).filter(item -> !item.isEmpty()).toList();
    }

    private static String rootMessage(Throwable error) {
        Throwable root = error;
        while ((root instanceof CompletionException || root instanceof ExecutionException) && root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
    }

    /// The client-facing reason the request is refused, or empty when it passes. Every refusal
    /// is logged with the API it came through.
    private Optional<String> refusal(String api, GovernanceRequest request) {
        Optional<String> refusal = reason(request);
        refusal.ifPresent(message -> LOG.info("governance refused {} for {} '{}' by principal '{}': {}",
                api, request.resourceType().name().toLowerCase(Locale.ROOT), request.resourceName(), request.principal(), message));
        return refusal;
    }

    private Optional<String> reason(GovernanceRequest request) {
        List<Violation> violations;
        try {
            violations = policy.get().evaluate(request);
        } catch (IllegalStateException e) {
            return Optional.of("governance could not evaluate the request: " + e.getMessage());
        }
        if (violations.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(violations.stream().map(Violation::describe).collect(Collectors.joining("; ")));
    }

    /// Entries refused on the way in, by topic or resource name, with their message; DeleteTopics
    /// may also name topics by id only.
    private record Refusals(Map<String, String> byName, Map<Uuid, String> byTopicId) {

        Refusals(Map<String, String> byName) {
            this(byName, Map.of());
        }
    }

    /// The topics a change touches as [#prepare] read them, or why they could not be read.
    private record CurrentTopics(Map<String, TopicState> byName, Throwable error) {
    }

    /// The partitions of an offset commit whose group was refused, by topic.
    private record CommitRefusal(Map<String, List<Integer>> partitionsByTopic) {
    }
}
