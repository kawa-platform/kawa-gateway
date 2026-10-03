package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Match;
import io.jonasg.kawa.config.GovernanceRuleConfig.Operation;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import io.jonasg.kawa.config.GovernanceRuleConfig.SubRule;
import io.jonasg.kawa.config.GovernanceRuleConfig.TopicScope;
import io.jonasg.kawa.config.GovernanceVariableConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;

/// Governance enforced on the Kafka protocol, end to end: a real `AdminClient` creating topics
/// and altering configs through the gateway against a real broker. Refused entries come back as
/// `PolicyViolationException` with the governance message and never reach the broker; the rest
/// of a request goes through.
///
/// The config is a small toy factory: topics are `workshop.<line>.<station>` or
/// `orders.<channel>.placed|shipped`, physical topics have 1 or 3 partitions, and
/// `workshop.qa.*` topics keep results for at least a day; sizing and retention also hold on
/// changes, judged against the topic as it will be after the change, and QA topics are never
/// deleted. Consumer
/// groups and transactional ids belong to a crew: `crew-<name>`.
class GovernanceIT extends AdminHTTPTestSupport {

    private static final String VISITOR = "visitor";
    private static final String VISITOR_PASSWORD = "visitor-secret";
    private static final String DAY_MS = "86400000";

    @Override
    protected List<NewTopic> initialTopics() {
        // Created on the broker directly, before governance: legacy_orders breaks naming, the
        // press topic sizing and the QA topic retention.
        return List.of(
                new NewTopic("legacy_orders", 1, (short) 1),
                new NewTopic("workshop.press.stamp", 2, (short) 1),
                new NewTopic("workshop.qa.legacy", 1, (short) 1).configs(Map.of("retention.ms", "1000")));
    }

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                Set.of("PLAIN"),
                Map.of(DEFAULT_PRINCIPAL, client("PLAIN", DEFAULT_PASSWORD), VISITOR, client("PLAIN", VISITOR_PASSWORD)),
                null);
    }

    @Override
    protected RbacConfig rbacConfig() {
        var allowAll = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.TRANSACTIONAL_ID, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        var readOnly = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.DESCRIBE)));
        return new RbacConfig(
                Map.of("allow-all", allowAll, "read-only", readOnly),
                Map.of(
                        "it-defaults", new GroupConfig(List.of(DEFAULT_PRINCIPAL), List.of("allow-all")),
                        "visitors", new GroupConfig(List.of(VISITOR), List.of("read-only"))));
    }

    @Override
    protected String groupId() {
        return "crew-shared";
    }

    @Override
    protected GovernanceConfig governanceConfig() {
        var naming = new GovernanceRuleConfig("naming",
                "Topic names must be workshop.<line>.<station> or orders.<channel>.placed|shipped.", null,
                Selector.topic(), Match.ANY,
                List.of(
                        SubRule.check("workshop", Expression.cel("topic.name.matches('^workshop\\\\.[a-z]+\\\\.[a-z0-9-]+$')")),
                        SubRule.group("orders", Match.ANY, List.of(
                                SubRule.check("placed", Expression.cel("topic.name.matches('^orders\\\\.[a-z]+\\\\.placed$')")),
                                SubRule.check("shipped", Expression.cel("topic.name.matches('^orders\\\\.[a-z]+\\\\.shipped$')"))))),
                List.of(new GovernanceRuleConfig.Exemption("unnamed-lab", "The lab names things freely.",
                        Expression.cel("topic.name.startsWith('lab-')"))));
        var sizing = new GovernanceRuleConfig("sizing", "Topic sizing does not meet factory standards.", null,
                new Selector(ResourceType.TOPIC, null, TopicScope.PHYSICAL, Set.of(Operation.CREATE, Operation.ALTER)), Match.ALL,
                List.of(new SubRule.Group("limits", null, Match.ALL, List.of(
                        new SubRule.Check("tier", "Partitions must be 1 or 3.", Expression.cel("topic.partitions in partitionTiers"))))),
                List.of());
        var qaRetention = new GovernanceRuleConfig("qa-retention", "QA topics keep results for at least a day.", null,
                new Selector(ResourceType.TOPIC, Expression.cel("topic.name.startsWith('workshop.qa.')"), TopicScope.PHYSICAL,
                        Set.of(Operation.CREATE, Operation.ALTER)),
                Expression.cel("!('retention.ms' in topic.configs) || int(topic.configs['retention.ms']) >= " + DAY_MS));
        var keepQa = new GovernanceRuleConfig("keep-qa", "QA results are never deleted.", null,
                new Selector(ResourceType.TOPIC, Expression.cel("topic.name.startsWith('workshop.qa.')"), TopicScope.PHYSICAL,
                        Set.of(Operation.DELETE)),
                Expression.cel("false"));
        var crewGroups = new GovernanceRuleConfig("crew-groups", "Consumer groups belong to a crew: crew-<name>.", null,
                new Selector(ResourceType.GROUP, null), Expression.cel("group.id.startsWith('crew-')"));
        var crewTransactions = new GovernanceRuleConfig("crew-transactions", "Transactional ids belong to a crew: crew-<name>.", null,
                new Selector(ResourceType.TRANSACTIONAL_ID, null), Expression.cel("transaction.id.startsWith('crew-')"));
        return new GovernanceConfig(
                Map.of("naming", naming, "sizing", sizing, "qa-retention", qaRetention,
                        "crew-groups", crewGroups, "crew-transactions", crewTransactions, "keep-qa", keepQa),
                Map.of("scratch", new GovernanceRuleConfig.Exemption("scratch", "Scratch space skips every rule.",
                        Expression.cel("topic.name.startsWith('scratch.')"))),
                Map.of("partitionTiers", new GovernanceVariableConfig(
                        "partitionTiers", GovernanceVariableConfig.Type.LIST_INT, "[1, 3]", "single or small")));
    }

    @Test
    void createsACompliantTopic() throws Exception {
        // when
        gatewayAdmin.createTopics(List.of(new NewTopic("workshop.robots.welding", 3, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // then
        assertThat(brokerTopics())
                .withFailMessage(() -> "Compliant topic 'workshop.robots.welding' was not created on the broker")
                .contains("workshop.robots.welding");
    }

    @Test
    void refusesANonCompliantTopicWithPolicyViolation() throws Exception {
        // when / then
        assertThatThrownBy(() -> gatewayAdmin.createTopics(List.of(new NewTopic("misc.things", 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[naming] Topic names must be workshop.<line>.<station>");
        assertThat(brokerTopics())
                .withFailMessage(() -> "Refused topic 'misc.things' reached the broker")
                .doesNotContain("misc.things");
    }

    @Test
    void namesTheFailingCheckAndItsOwnMessage() {
        // when / then
        assertThatThrownBy(() -> gatewayAdmin.createTopics(List.of(new NewTopic("workshop.robots.paint", 2, (short) 1)))
                .all().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[sizing > limits > tier] Partitions must be 1 or 3.");
    }

    @Test
    void createsTheGoodTopicsOfAMixedBatch() throws Exception {
        // when
        var result = gatewayAdmin.createTopics(List.of(
                new NewTopic("orders.web.placed", 1, (short) 1),
                new NewTopic("orders.web.lost", 1, (short) 1)));

        // then
        result.values().get("orders.web.placed").get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> result.values().get("orders.web.lost").get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class);
        assertThat(brokerTopics())
                .withFailMessage(() -> "Batch did not end with only the compliant topic on the broker")
                .contains("orders.web.placed")
                .doesNotContain("orders.web.lost");
    }

    @Test
    void ruleExemptionSkipsItsRuleAndGlobalExemptionSkipsAll() throws Exception {
        // when - naming is exempt for lab-*, sizing still applies (3 partitions passes)
        gatewayAdmin.createTopics(List.of(new NewTopic("lab-bouncy-ball", 3, (short) 1))).all().get(10, TimeUnit.SECONDS);
        // and - scratch.* skips every rule, including sizing (2 partitions)
        gatewayAdmin.createTopics(List.of(new NewTopic("scratch.anything", 2, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // then
        assertThat(brokerTopics()).contains("lab-bouncy-ball", "scratch.anything");
    }

    @Test
    void ruleExemptionDoesNotSkipOtherRules() {
        // when / then - exempt from naming, but 2 partitions still breaks sizing
        assertThatThrownBy(() -> gatewayAdmin.createTopics(List.of(new NewTopic("lab-yo-yo", 2, (short) 1)))
                .all().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[sizing > limits > tier]");
    }

    @Test
    void refusesAConfigChangeThatBreaksAnAlterRule() throws Exception {
        // given
        gatewayAdmin.createTopics(List.of(new NewTopic("workshop.qa.inspect", 1, (short) 1)
                .configs(Map.of("retention.ms", DAY_MS)))).all().get(10, TimeUnit.SECONDS);
        var topic = new ConfigResource(ConfigResource.Type.TOPIC, "workshop.qa.inspect");

        // when / then - shortening retention to a second is refused
        assertThatThrownBy(() -> gatewayAdmin.incrementalAlterConfigs(Map.of(topic, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", "1000"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[qa-retention] QA topics keep results for at least a day.");
        assertThat(retentionMs("workshop.qa.inspect"))
                .withFailMessage(() -> "Refused config change reached the broker")
                .isEqualTo(DAY_MS);

        // and - lengthening it is fine
        gatewayAdmin.incrementalAlterConfigs(Map.of(topic, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", "172800000"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
        assertThat(retentionMs("workshop.qa.inspect")).isEqualTo("172800000");
    }

    @Test
    void createOnlyRulesDoNotBlockConfigChanges() throws Exception {
        // given - legacy_orders breaks naming, which runs on create only
        var topic = new ConfigResource(ConfigResource.Type.TOPIC, "legacy_orders");

        // when
        gatewayAdmin.incrementalAlterConfigs(Map.of(topic, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", "3600000"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);

        // then
        assertThat(retentionMs("legacy_orders")).isEqualTo("3600000");
    }

    @Test
    void refusesAddingPartitionsBeyondTheTiers() throws Exception {
        // given
        gatewayAdmin.createTopics(List.of(new NewTopic("workshop.robots.lift", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // when / then - 2 partitions breaks sizing
        assertThatThrownBy(() -> gatewayAdmin.createPartitions(Map.of("workshop.robots.lift", NewPartitions.increaseTo(2)))
                .all().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[sizing > limits > tier] Partitions must be 1 or 3.");
        assertThat(partitions("workshop.robots.lift"))
                .withFailMessage(() -> "Refused partitions were added on the broker")
                .isEqualTo(1);

        // and - 3 is a tier
        gatewayAdmin.createPartitions(Map.of("workshop.robots.lift", NewPartitions.increaseTo(3))).all().get(10, TimeUnit.SECONDS);
        assertThat(partitions("workshop.robots.lift")).isEqualTo(3);
    }

    @Test
    void judgesAConfigChangeWithTheTopicsCurrentConfigs() throws Exception {
        // given - the QA topic keeps results for a second today
        var topic = new ConfigResource(ConfigResource.Type.TOPIC, "workshop.qa.legacy");

        // when / then - a change that leaves retention alone is still refused
        assertThatThrownBy(() -> gatewayAdmin.incrementalAlterConfigs(Map.of(topic, List.of(
                        new AlterConfigOp(new ConfigEntry("segment.ms", "3600000"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS))
                .withFailMessage(() -> "A change was judged without the topic's current retention.ms")
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[qa-retention]");

        // and - a change that fixes retention goes through
        gatewayAdmin.incrementalAlterConfigs(Map.of(topic, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", DAY_MS), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
        assertThat(retentionMs("workshop.qa.legacy")).isEqualTo(DAY_MS);
    }

    @Test
    void judgesAConfigChangeWithTheTopicsPartitions() {
        // given - the press topic has 2 partitions, outside the tiers
        var topic = new ConfigResource(ConfigResource.Type.TOPIC, "workshop.press.stamp");

        // when / then
        assertThatThrownBy(() -> gatewayAdmin.incrementalAlterConfigs(Map.of(topic, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", DAY_MS), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[sizing > limits > tier]");
    }

    @Test
    void refusesDeletingAProtectedTopic() throws Exception {
        // given
        gatewayAdmin.createTopics(List.of(
                new NewTopic("workshop.qa.archive", 1, (short) 1),
                new NewTopic("workshop.robots.retired", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // when / then - QA results stay
        assertThatThrownBy(() -> gatewayAdmin.deleteTopics(List.of("workshop.qa.archive")).all().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PolicyViolationException.class)
                .cause().hasMessageContaining("[keep-qa] QA results are never deleted.");
        assertThat(brokerTopics())
                .withFailMessage(() -> "A protected topic was deleted on the broker")
                .contains("workshop.qa.archive");

        // and - other topics can go
        gatewayAdmin.deleteTopics(List.of("workshop.robots.retired")).all().get(10, TimeUnit.SECONDS);
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(brokerTopics()).doesNotContain("workshop.robots.retired"));
    }

    @Test
    void picksUpARuleAddedAtRuntime() throws Exception {
        // given - puzzles are fine so far
        gatewayAdmin.createTopics(List.of(new NewTopic("workshop.puzzles.cut", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // when - a rule retiring the puzzle line is written through the admin API
        var put = httpExec(reqBuilder("/governance/rules/puzzles-retired?consistency=applied")
                .PUT(HttpRequest.BodyPublishers.ofString("""
                        {
                          "errorMessage": "The puzzle line is retired.",
                          "selector": {"resourceType": "TOPIC"},
                          "match": "ALL",
                          "subRules": [{"kind": "check", "name": "not-puzzles",
                                        "expression": {"type": "CEL", "value": "!topic.name.startsWith('workshop.puzzles.')"}}]
                        }
                        """))
                .header("content-type", "application/json")
                .build());
        assertThat(put).hasStatusCode(200);

        // then
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThatThrownBy(() -> gatewayAdmin.createTopics(List.of(new NewTopic("workshop.puzzles.glue", 1, (short) 1)))
                        .all().get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(PolicyViolationException.class)
                        .cause().hasMessageContaining("[puzzles-retired > not-puzzles] The puzzle line is retired."));
    }

    @Test
    void rbacRefusesBeforeGovernanceIsAsked() {
        // given
        Properties props = saslProps(gatewayBootstrap);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + VISITOR + "\" password=\"" + VISITOR_PASSWORD + "\";");

        // when / then - the name also breaks naming, but RBAC answers first
        try (var visitor = AdminClient.create(props)) {
            assertThatThrownBy(() -> visitor.createTopics(List.of(new NewTopic("misc.visitor", 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ClusterAuthorizationException.class);
        }
    }

    @Test
    void refusesAConsumerGroupOutsideTheCrews() {
        // given
        try (var consumer = consumer("night-shift")) {
            consumer.subscribe(List.of("legacy_orders"));

            // when / then
            assertThatThrownBy(() -> {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (System.nanoTime() < deadline) {
                    consumer.poll(Duration.ofMillis(200));
                }
            })
                    .withFailMessage(() -> "Consumer in group 'night-shift' joined although groups must be crew-<name>")
                    .isInstanceOf(InvalidGroupIdException.class);
        }
    }

    @Test
    void letsACrewConsumerGroupJoin() {
        // given
        try (var consumer = consumer("crew-packers")) {
            consumer.subscribe(List.of("legacy_orders"));

            // when / then
            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(200));
                assertThat(consumer.assignment())
                        .withFailMessage(() -> "Crew group 'crew-packers' got no partitions")
                        .isNotEmpty();
            });
        }
    }

    @Test
    void refusesATransactionalIdOutsideTheCrews() {
        // given
        try (var producer = transactionalProducer("night-shift-tx")) {

            // when / then
            assertThatThrownBy(producer::initTransactions)
                    .withFailMessage(() -> "Transactional id 'night-shift-tx' was initialised although ids must be crew-<name>")
                    .isInstanceOf(TransactionalIdAuthorizationException.class);
        }
    }

    @Test
    void letsACrewTransactionalIdInitialise() {
        // given
        try (var producer = transactionalProducer("crew-packers-tx")) {

            // when / then
            producer.initTransactions();
        }
    }

    private static KafkaConsumer<String, String> consumer(String group) {
        Properties props = saslProps(gatewayBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> transactionalProducer(String transactionalId) {
        Properties props = saslProps(gatewayBootstrap);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    private static Collection<String> brokerTopics() throws Exception {
        return brokerAdmin.listTopics().names().get(10, TimeUnit.SECONDS);
    }

    private static int partitions(String topic) throws Exception {
        return brokerAdmin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic).partitions().size();
    }

    private static String retentionMs(String topic) throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        return brokerAdmin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS)
                .get(resource).get("retention.ms").value();
    }
}
