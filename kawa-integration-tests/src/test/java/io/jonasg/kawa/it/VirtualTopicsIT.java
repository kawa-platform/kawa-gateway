package io.jonasg.kawa.it;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kassert.Kassertions;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// End-to-end test for virtual topics: a producer talks to the gateway, which rewrites the
/// virtual topic name to its physical topic before forwarding to the broker.
class VirtualTopicsIT extends GatewayTestSupport {

    @Override
    protected Map<String, String> virtualTopics() {
        return Map.of("foo", "foo-v2", "session", "session-v3");
    }

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of("events", new VirtualTopicConfig("events-v2",
                new HeaderEqualsFilterConfig("tenant", "acme")));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("foo-v2", 1, (short) 1)
                        .configs(Map.of("retention.ms", "987654321")),
                new NewTopic("bar", 1, (short) 1),
                new NewTopic("session-v3", 1, (short) 1),
                new NewTopic("events-v2", 1, (short) 1));
    }

    @Override
    protected String groupId() {
        return "virtual-topics-it";
    }

    @Nested
    class ProduceOnVirtualTopicThroughGateway {

        private String value;

        @BeforeEach
        void produceOnVirtualTopicThroughGateway() throws ExecutionException, InterruptedException, TimeoutException {
            this.value = "hello-" + System.nanoTime();

            gatewayProducer.send(new ProducerRecord<>("foo", "key", this.value)).get(5, TimeUnit.SECONDS);
        }

        @Test
        void consumeFromPhysicalThroughBroker() {
            Kassertions.consume(brokerConsumer)
                    .assignedTo("foo-v2")
                    .fromBeginning()
                    .filter(rec -> rec.topic().equals("foo-v2"))
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo(this.value));
        }

        @Test
        void consumeFromPhysicalThroughGateway() {
            Kassertions.consume(gatewayConsumer)
                    .assignedTo("foo")
                    .fromBeginning()
                    .filter(rec -> rec.topic().equals("foo"))
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo(this.value));
        }

        @Test
        void consumeFromVirtualTopicThroughGateway() {
            Kassertions.consume(gatewayConsumer)
                    .assignedTo("foo")
                    .fromBeginning()
                    .filter(rec -> rec.topic().equals("foo"))
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo(this.value));
        }
    }

    @Test
    void listsVirtualOnlyTopicsThroughGateway() throws Exception {
        assertThat(gatewayAdmin.listTopics().names().get())
                .describedAs("client must see virtual topics only")
                .contains("foo", "bar", "events");
    }

    @Test
    void creatingVirtualTopicNameFails() throws Exception {
        var result = gatewayAdmin.createTopics(List.of(new NewTopic("foo", 1, (short) 1)));
        assertThatThrownBy(() -> result.values().get("foo").get())
                .hasCauseInstanceOf(InvalidRequestException.class);
    }

    @Test
    void creatingPhysicalTopicThroughGatewaySucceeds() throws Exception {
        String topic = "baz-" + System.nanoTime();
        gatewayAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(5, TimeUnit.SECONDS);
        eventually(() -> assertThat(brokerAdmin.listTopics().names().get()).contains(topic));
    }

    @Test
    void deletingVirtualTopicNameFails() throws Exception {
        var result = gatewayAdmin.deleteTopics(List.of("foo"));

        assertThatThrownBy(() -> result.all().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(InvalidRequestException.class)
                .hasRootCauseMessage("virtual topic 'foo' is reserved; use 'foo-v2'");
    }

    @Test
    void deletingPhysicalTopicThroughGatewaySucceeds() throws Exception {
        String topic = "baz-delete-" + System.nanoTime();
        brokerAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(5, TimeUnit.SECONDS);

        gatewayAdmin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(gatewayAdmin.listTopics().names().get()).doesNotContain(topic));
    }

    @Test
    void creatingPartitionsOnVirtualTopicUpdatesPhysicalTopic() throws Exception {
        String physical = "foo-v2";
        int current = brokerAdmin.describeTopics(List.of(physical)).allTopicNames().get()
                .get(physical).partitions().size();

        gatewayAdmin.createPartitions(Map.of("foo", NewPartitions.increaseTo(current + 1)))
                .all().get(5, TimeUnit.SECONDS);

        eventually(() -> assertThat(brokerAdmin.describeTopics(List.of(physical)).allTopicNames().get()
                .get(physical).partitions()).hasSize(current + 1));
    }

    @Test
    void describeConfigsByVirtualTopicNameReturnsPhysicalTopicConfigs() throws Exception {
        // given the physical topic foo-v2 carries retention.ms=987654321
        ConfigResource virtual = new ConfigResource(ConfigResource.Type.TOPIC, "foo");

        // when describing configs for the virtual topic through the gateway
        Map<ConfigResource, Config> configs = gatewayAdmin
                .describeConfigs(List.of(virtual))
                .all().get(5, TimeUnit.SECONDS);

        // then the physical topic's config is returned under the virtual name
        assertThat(configs).containsKey(virtual);
        assertThat(configs.get(virtual).get("retention.ms").value())
                .describedAs("configs returned under the virtual name must be the physical topic's configs")
                .isEqualTo("987654321");
    }

    @Test
    void creatingAclForVirtualTopicAppliesToPhysicalTopic() throws Exception {
        // given an acl binding for the virtual topic foo
        AclBinding virtualBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));

        // when creating the acl through the gateway
        gatewayAdmin.createAcls(List.of(virtualBinding)).all().get(5, TimeUnit.SECONDS);

        // then the acl is stored under the physical topic name, not the virtual one
        eventually(() -> {
            Collection<AclBinding> physical = brokerAdmin.describeAcls(new AclBindingFilter(
                            new ResourcePatternFilter(ResourceType.TOPIC, "foo-v2", PatternType.LITERAL),
                            AccessControlEntryFilter.ANY))
                    .values().get(5, TimeUnit.SECONDS);
            assertThat(physical).hasSize(1);
            assertThat(physical.iterator().next().pattern().name()).isEqualTo("foo-v2");
            assertThat(physical.iterator().next().entry().principal()).isEqualTo("User:alice");
        });

        Collection<AclBinding> virtual = brokerAdmin.describeAcls(new AclBindingFilter(
                        new ResourcePatternFilter(ResourceType.TOPIC, "foo", PatternType.LITERAL),
                        AccessControlEntryFilter.ANY))
                .values().get(5, TimeUnit.SECONDS);
        assertThat(virtual).isEmpty();
    }

    @Test
    void describingAclsWithVirtualTopicFilterReturnsVirtualTopicNames() throws Exception {
        // given an acl on the physical topic foo-v2
        brokerAdmin.createAcls(List.of(new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "foo-v2", PatternType.LITERAL),
                        new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW))))
                .all().get(5, TimeUnit.SECONDS);

        // when describing acls for the virtual topic through the gateway
        // then the acl is returned under the virtual topic name
        eventually(() -> {
            Collection<AclBinding> described = gatewayAdmin.describeAcls(new AclBindingFilter(
                            new ResourcePatternFilter(ResourceType.TOPIC, "foo", PatternType.LITERAL),
                            AccessControlEntryFilter.ANY))
                    .values().get(5, TimeUnit.SECONDS);
            assertThat(described).hasSize(1);
            AclBinding binding = described.iterator().next();
            assertThat(binding.pattern().name()).isEqualTo("foo");
            assertThat(binding.pattern().patternType()).isEqualTo(PatternType.LITERAL);
            assertThat(binding.entry().principal()).isEqualTo("User:alice");
            assertThat(binding.entry().operation()).isEqualTo(AclOperation.READ);
        });
    }

    @Test
    void deletingAclsByVirtualTopicFilterRemovesPhysicalTopicAcl() throws Exception {
        // given acls on the physical topic foo-v2 and on group "orders"
        brokerAdmin.createAcls(List.of(
                        new AclBinding(
                                new ResourcePattern(ResourceType.TOPIC, "foo-v2", PatternType.LITERAL),
                                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW)),
                        new AclBinding(
                                new ResourcePattern(ResourceType.GROUP, "orders", PatternType.LITERAL),
                                new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW))))
                .all().get(5, TimeUnit.SECONDS);
        eventually(() -> assertThat(brokerAdmin.describeAcls(AclBindingFilter.ANY).values().get(5, TimeUnit.SECONDS))
                .extracting(binding -> binding.pattern().name())
                .contains("foo-v2", "orders"));

        // when deleting acls filtered by the virtual topic name through the gateway
        Collection<AclBinding> deleted = gatewayAdmin.deleteAcls(List.of(new AclBindingFilter(
                        new ResourcePatternFilter(ResourceType.TOPIC, "foo", PatternType.LITERAL),
                        AccessControlEntryFilter.ANY)))
                .all().get(5, TimeUnit.SECONDS);

        // then the deleted bindings are reported under the virtual name and removed physically
        assertThat(deleted).hasSize(1);
        assertThat(deleted.iterator().next().pattern().name()).isEqualTo("foo");

        eventually(() -> {
            Collection<AclBinding> remaining = brokerAdmin.describeAcls(AclBindingFilter.ANY)
                    .values().get(5, TimeUnit.SECONDS);
            assertThat(remaining).hasSize(1);
            assertThat(remaining.iterator().next().pattern().resourceType()).isEqualTo(ResourceType.GROUP);
            assertThat(remaining.iterator().next().pattern().name()).isEqualTo("orders");
        });
    }

    @Test
    void describeTransactionsReportsVirtualTopicNames() throws Exception {
        // given an open transaction on the physical topic, created directly against the broker
        String transactionalId = "describe-txn-" + System.nanoTime();
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        try (KafkaProducer<String, String> txnProducer = new KafkaProducer<>(producerProps)) {
            txnProducer.initTransactions();
            txnProducer.beginTransaction();
            txnProducer.send(new ProducerRecord<>("foo-v2", 0, "key", "in-flight"))
                    .get(5, TimeUnit.SECONDS);

            // when describing the transaction through the gateway
            Map<String, TransactionDescription> described = gatewayAdmin
                    .describeTransactions(List.of(transactionalId))
                    .all().get(5, TimeUnit.SECONDS);

            // then the touched topic is reported under its virtual name
            TransactionDescription description = described.get(transactionalId);
            assertThat(description.topicPartitions())
                    .describedAs("in-flight transaction must report the virtual topic name")
                    .containsExactly(new TopicPartition("foo", 0));

            txnProducer.abortTransaction();
        }
    }

    @Test
    void transactionalProduceToVirtualTopicCommitsAndIsConsumable() throws Exception {
        // given a transactional producer pointed at the gateway
        String transactionalId = "txn-produce-" + System.nanoTime();
        Properties producerProps = saslProps(gatewayBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        try (KafkaProducer<String, String> txnProducer = new KafkaProducer<>(producerProps)) {
            txnProducer.initTransactions();
            txnProducer.beginTransaction();
            txnProducer.send(new ProducerRecord<>("foo", 0, "key", "committed-via-gateway"))
                    .get(5, TimeUnit.SECONDS);
            txnProducer.commitTransaction();

            // when consuming the virtual topic through the gateway
            Kassertions.consume(newGatewayConsumer())
                    .assignedTo("foo", 0)
                    .fromBeginning()
                    .within(5, TimeUnit.SECONDS)
                    .filter(rec -> "committed-via-gateway".equals(rec.value()))
                    .anySatisfy(rec -> assertThat(rec.topic())
                            .describedAs("consumed records must carry the virtual topic name")
                            .isEqualTo("foo"));

            // then describing the committed transaction reports no in-flight partitions
            Map<String, TransactionDescription> described = gatewayAdmin
                    .describeTransactions(List.of(transactionalId))
                    .all().get(5, TimeUnit.SECONDS);
            assertThat(described.get(transactionalId).topicPartitions()).isEmpty();
        }
    }

    @Test
    void deletingOffsetsForVirtualTopicDeletesPhysicalTopicOffsets() throws Exception {
        // given a group with a committed offset on the virtual topic, stored via the gateway
        String group = "offset-delete-" + System.nanoTime();
        TopicPartition virtual = new TopicPartition("session", 0);
        gatewayAdmin.alterConsumerGroupOffsets(group, Map.of(virtual, new OffsetAndMetadata(2L)))
                .all().get(5, TimeUnit.SECONDS);

        // when deleting the offset for the virtual topic through the gateway
        gatewayAdmin.deleteConsumerGroupOffsets(group, java.util.Set.of(virtual))
                .all().get(5, TimeUnit.SECONDS);

        // then the physical topic's offset is gone on the broker
        Map<TopicPartition, OffsetAndMetadata> remaining = brokerAdmin
                .listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata(group).get(5, TimeUnit.SECONDS);
        assertThat(remaining)
                .describedAs("offset deleted under the virtual name must be gone for the physical topic")
                .isEmpty();
    }

    @Test
    void alteringConfigsByVirtualTopicNameAffectsPhysicalTopic() throws Exception {
        // when altering configs for the virtual topic through the gateway
        ConfigResource virtual = new ConfigResource(ConfigResource.Type.TOPIC, "session");
        gatewayAdmin.incrementalAlterConfigs(Map.of(virtual, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", "111111111"),
                                AlterConfigOp.OpType.SET))))
                .all().get(5, TimeUnit.SECONDS);

        // then the physical topic carries the new config value
        eventually(() -> {
            Map<ConfigResource, Config> configs = brokerAdmin
                    .describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, "session-v3")))
                    .all().get(5, TimeUnit.SECONDS);
            assertThat(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, "session-v3"))
                    .get("retention.ms").value())
                    .describedAs("config altered under the virtual name must land on the physical topic")
                    .isEqualTo("111111111");
        });
    }

    @Test
    void incrementallyAlteringConfigsByVirtualTopicNameAffectsPhysicalTopic() throws Exception {
        // when incrementally setting a config on the virtual topic through the gateway
        ConfigResource virtual = new ConfigResource(ConfigResource.Type.TOPIC, "session");
        gatewayAdmin.incrementalAlterConfigs(Map.of(virtual, List.of(
                        new AlterConfigOp(new ConfigEntry("retention.ms", "222222222"),
                                AlterConfigOp.OpType.SET))))
                .all().get(5, TimeUnit.SECONDS);

        // then the physical topic carries the incremented config value
        eventually(() -> {
            Map<ConfigResource, Config> configs = brokerAdmin
                    .describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, "session-v3")))
                    .all().get(5, TimeUnit.SECONDS);
            assertThat(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, "session-v3"))
                    .get("retention.ms").value())
                    .describedAs("incremental config change must land on the physical topic")
                    .isEqualTo("222222222");
        });
    }

    @Test
    void deletingRecordsByVirtualTopicTruncatesPhysicalTopic() throws Exception {
        // given five records produced to the virtual topic through the gateway
        for (int i = 0; i < 5; i++) {
            gatewayProducer.send(new ProducerRecord<>("session", "k" + i, "truncate-" + i))
                    .get(5, TimeUnit.SECONDS);
        }

        // when deleting records up to offset 3 via the virtual name through the gateway
        var lowWatermarks = gatewayAdmin
                .deleteRecords(Map.of(new TopicPartition("session", 0),
                        RecordsToDelete.beforeOffset(3L)))
                .lowWatermarks();
        long lowWatermark = lowWatermarks.get(new TopicPartition("session", 0))
                .get(5, TimeUnit.SECONDS).lowWatermark();

        // then the response reports the virtual name and the physical topic is truncated
        assertThat(lowWatermark)
                .describedAs("delete-records must succeed under the virtual topic name")
                .isEqualTo(3L);
        long beginning = brokerAdmin
                .listOffsets(Map.of(new TopicPartition("session-v3", 0),
                        org.apache.kafka.clients.admin.OffsetSpec.earliest()))
                .all().get(5, TimeUnit.SECONDS)
                .get(new TopicPartition("session-v3", 0)).offset();
        assertThat(beginning)
                .describedAs("physical topic's log start offset must have moved to 3")
                .isEqualTo(3L);
    }

    @Test
    void consumingFilteredVirtualTopicReturnsOnlyMatchingRecords() throws Exception {
        // given records with mixed header values produced directly to the physical topic
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        String marker = "filtered-" + System.nanoTime();
        try (KafkaProducer<String, String> rawProducer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> acme1 =
                    new ProducerRecord<>("events-v2", 0, "k1", marker + "-acme-1");
            acme1.headers().add("tenant", "acme".getBytes(StandardCharsets.UTF_8));
            ProducerRecord<String, String> other1 =
                    new ProducerRecord<>("events-v2", 0, "k2", marker + "-other-1");
            other1.headers().add("tenant", "other".getBytes(StandardCharsets.UTF_8));
            ProducerRecord<String, String> acme2 =
                    new ProducerRecord<>("events-v2", 0, "k3", marker + "-acme-2");
            acme2.headers().add("tenant", "acme".getBytes(StandardCharsets.UTF_8));
            rawProducer.send(acme1).get(5, TimeUnit.SECONDS);
            rawProducer.send(other1).get(5, TimeUnit.SECONDS);
            rawProducer.send(acme2).get(5, TimeUnit.SECONDS);
        }

        // when consuming the virtual topic through the gateway
        java.util.List<String> values = new java.util.ArrayList<>();
        Properties consumerProps = saslProps(gatewayBootstrap);
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            TopicPartition virtual = new TopicPartition("events", 0);
            consumer.assign(List.of(virtual));
            consumer.seekToBeginning(List.of(virtual));
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline
                   && values.stream().noneMatch(v -> v.startsWith(marker))) {
                consumer.poll(java.time.Duration.ofMillis(200))
                        .forEach(record -> values.add(record.value()));
            }
        }

        // then only the matching records are delivered, non-matching ones are dropped
        assertThat(values)
                .describedAs("only records whose tenant header equals 'acme' may be consumed")
                .contains(marker + "-acme-1", marker + "-acme-2")
                .doesNotContain(marker + "-other-1");
    }

    @Test
    void consumerNeverSeesPhysicalTopicNameAcrossIdleFetchSession() {
        String value = "idle-" + System.nanoTime();
        produceAfterDelay("session", "key", value, 1500);

        Kassertions.consume(newGatewayConsumer())
                .assignedTo("session", 0)
                .fromBeginning()
                .within(5, TimeUnit.SECONDS)
                .filter(rec -> rec.topic().equals("session"))
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo(value));
    }

    @Test
    void idleIncrementalFetchResponseCarriesVirtualTopicName() throws Exception {
        String value = "wire-" + System.nanoTime();
        TopicPartition session = new TopicPartition("session", 0);

        try (RawFetchSession raw = new RawFetchSession(gatewayBootstrap)) {
            FetchResponse created = raw.fetch(
                    Map.of(session,
                            new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 1024 * 1024, Optional.empty())),
                    FetchMetadata.INITIAL, 500);
            assertThat(created.sessionId())
                    .describedAs("full fetch must establish a fetch session")
                    .isNotZero();

            gatewayProducer.send(new ProducerRecord<>("session", "key", value)).get(5, TimeUnit.SECONDS);

            FetchResponse incremental = raw.fetch(Map.of(),
                    new FetchMetadata(created.sessionId(), 1), 1000);

            assertThat(incremental.data().responses())
                    .describedAs("idle incremental fetch must carry the produced record back")
                    .isNotEmpty();
            FetchResponseData.FetchableTopicResponse response = incremental.data().responses().get(0);
            assertThat(response.topic())
                    .describedAs("idle incremental fetch response must rename physical topic back to virtual topic")
                    .isEqualTo("session");
        }
    }

    @Test
    void transactionalOffsetCommitRewritesVirtualTopicToPhysical() throws Exception {
        String transactionalId = "txn-" + System.nanoTime();
        String group = "txn-group-" + System.nanoTime();

        Properties producerProps = saslProps(gatewayBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        try (KafkaProducer<String, String> txnProducer = new KafkaProducer<>(producerProps)) {
            txnProducer.initTransactions();
            txnProducer.beginTransaction();
            txnProducer.sendOffsetsToTransaction(
                    Map.of(new TopicPartition("foo", 0), new OffsetAndMetadata(5L)),
                    new ConsumerGroupMetadata(group));
            txnProducer.commitTransaction();
        }

        TopicPartition physicalPartition = new TopicPartition("foo-v2", 0);
        Map<TopicPartition, OffsetAndMetadata> physicalOffsets = brokerAdmin
                .listConsumerGroupOffsets(Map.of(group,
                        new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(physicalPartition))))
                .partitionsToOffsetAndMetadata(group).get(5, TimeUnit.SECONDS);
        assertThat(physicalOffsets.get(physicalPartition).offset())
                .describedAs("broker must see the transactional offset commit under the physical topic name")
                .isEqualTo(5L);

        TopicPartition virtualPartition = new TopicPartition("foo", 0);
        Map<TopicPartition, OffsetAndMetadata> virtualOffsets = gatewayAdmin
                .listConsumerGroupOffsets(Map.of(group,
                        new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(virtualPartition))))
                .partitionsToOffsetAndMetadata(group).get(5, TimeUnit.SECONDS);
        assertThat(virtualOffsets.get(virtualPartition).offset())
                .describedAs("gateway client must see the committed offset under the virtual topic name")
                .isEqualTo(5L);
    }

    private KafkaConsumer<String, String> newGatewayConsumer() {
        Properties props = saslProps(gatewayBootstrap);
        props.put(GROUP_ID_CONFIG, groupId() + "-" + System.nanoTime());
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private void produceAfterDelay(String topic, String key, String value, long delayMillis) {
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMillis);
                gatewayProducer.send(new ProducerRecord<>(topic, key, value)).get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /// Minimal raw TCP client that drives Kafka fetch sessions over the wire (no consumer),
    /// so the actual bytes the gateway forwards can be inspected without kafka-clients
    /// discarding or rewriting them. Uses Fetch v11 (the version modern consumers negotiate).
    private static final class RawFetchSession implements AutoCloseable {

        private static final short VERSION = 11;

        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        private int correlationId = 1;

        RawFetchSession(String bootstrap) throws IOException {
            String[] parts = bootstrap.split(":");
            int port = Integer.parseInt(parts[1]);
            Socket chosen = null;
            for (InetAddress address : InetAddress.getAllByName(parts[0])) {
                try {
                    chosen = new Socket();
                    chosen.connect(new InetSocketAddress(address, port), 5000);
                    break;
                } catch (IOException ignored) {
                }
            }
            if (chosen == null) {
                throw new IOException("could not connect to " + bootstrap);
            }
            this.socket = chosen;
            this.socket.setSoTimeout(10_000);
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());
            authenticate();
        }

        /// Performs the SASL handshake and PLAIN authentication over the raw socket, since RBAC
        /// is always enforced and an unauthenticated fetch is denied.
        private void authenticate() throws IOException {
            RequestHeader handshakeHeader =
                    new RequestHeader(ApiKeys.SASL_HANDSHAKE, (short) 1, "kawa-raw-it", correlationId++);
            var handshake = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"), (short) 1);
            write(handshake.serializeWithHeader(handshakeHeader));
            readResponse(ApiKeys.SASL_HANDSHAKE, (short) 1);

            RequestHeader authHeader =
                    new RequestHeader(ApiKeys.SASL_AUTHENTICATE, (short) 2, "kawa-raw-it", correlationId++);
            String payload = "\u0000" + DEFAULT_PRINCIPAL + "\u0000" + DEFAULT_PASSWORD;
            var authenticate = new SaslAuthenticateRequest(
                    new SaslAuthenticateRequestData().setAuthBytes(payload.getBytes(StandardCharsets.UTF_8)),
                    (short) 2);
            write(authenticate.serializeWithHeader(authHeader));
            readResponse(ApiKeys.SASL_AUTHENTICATE, (short) 2);
        }

        private void write(ByteBuffer payload) throws IOException {
            out.writeInt(payload.remaining());
            out.write(payload.array(), payload.position(), payload.remaining());
            out.flush();
        }

        private void readResponse(ApiKeys apiKey, short version) throws IOException {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            short responseHeaderVersion = apiKey.responseHeaderVersion(version);
            ResponseHeader.parse(buffer, responseHeaderVersion);
            if (apiKey == ApiKeys.SASL_AUTHENTICATE) {
                SaslAuthenticateResponse response = SaslAuthenticateResponse.parse(new ByteBufferAccessor(buffer), version);
                if (response.data().errorCode() != Errors.NONE.code()) {
                    throw new IOException("SASL authentication failed: " + response.data().errorMessage());
                }
            }
        }

        FetchResponse fetch(Map<TopicPartition, FetchRequest.PartitionData> toFetch, FetchMetadata metadata,
                            int maxWaitMs) throws IOException {
            RequestHeader header = new RequestHeader(ApiKeys.FETCH, VERSION, "kawa-raw-it", correlationId++);
            FetchRequest request = FetchRequest.Builder.forConsumer(VERSION, maxWaitMs, 1, toFetch)
                    .metadata(metadata)
                    .build();
            ByteBuffer payload = request.serializeWithHeader(header);
            out.writeInt(payload.remaining());
            out.write(payload.array(), payload.position(), payload.remaining());
            out.flush();

            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            buffer.position(4);
            return FetchResponse.parse(new ByteBufferAccessor(buffer), VERSION);
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
