package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ConfigTopicWriter;
import io.jonasg.kawa.config.ConfigWriteFencedException;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.governance.Violation;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.AuthenticationResult;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import io.jonasg.kawa.server.netty.ClientSession;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamicConfigManagerTest {

    private static GatewayConfig config(
            Map<String, VirtualTopicConfig> virtualTopics,
            RbacConfig rbac,
            AuthConfig auth,
            GovernanceConfig governance
    ) {
        return new GatewayConfig(null, null, virtualTopics, null, auth, rbac, null, null, governance);
    }

    private static RbacConfig rbacAllowingReadOnOrders() {
        return new RbacConfig(
                Map.of("reader", new RoleConfig(List.of(
                        new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                                AclOperation.READ, AclPermissionType.ALLOW)))),
                Map.of("team", new GroupConfig(List.of("alice"), List.of("reader"))));
    }

    private static AuthConfig plainAuth() {
        return new AuthConfig(Set.of("PLAIN"), Map.of("alice", new ClientConfig("PLAIN",
                HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret"))), null);
    }

    private static GovernanceRuleConfig rule(String name, String message, String expression) {
        return new GovernanceRuleConfig(
                name,
                message,
                message,
                Selector.topic(Expression.cel("true")),
                Expression.cel(expression));
    }

    private static GovernancePolicy emptyGovernance() {
        return new GovernancePolicy(new GovernanceConfig(null));
    }

    @Test
    void appliesSnapshotToAllConsumers() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        var authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        var sasl = new SaslAuthenticator();
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                virtualTopics, authorizer, sasl, emptyGovernance());
        var config = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth(), null);

        // when
        manager.apply(config);

        // then
        assertThat(virtualTopics.toPhysical("orders")).isEqualTo("orders-v2");
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
        var session = new ClientSession(new EmbeddedChannel());
        var handshake = sasl.handleHandshake(session, new SaslHandshakeRequestData().setMechanism("PLAIN"));
        assertThat(handshake.errorCode()).isEqualTo(Errors.NONE.code());
        var authenticate = sasl.handleAuthenticate(session, new SaslAuthenticateRequestData()
                .setAuthBytes("\u0000alice\u0000secret".getBytes(StandardCharsets.UTF_8)));
        assertThat(authenticate).isInstanceOf(AuthenticationResult.Success.class);
        assertThat(manager.getActiveConfig()).isSameAs(config);
    }

    @Test
    void rejectedSnapshotLeavesPreviousStateIntact() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        var authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        var sasl = new SaslAuthenticator();
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                virtualTopics, authorizer, sasl, emptyGovernance());
        var good = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth(), null);
        manager.apply(good);
        var broken = config(
                Map.of("customers", new VirtualTopicConfig("crm.customers")),
                new RbacConfig(Map.of(), Map.of("team", new GroupConfig(List.of("alice"), List.of("missing-role")))),
                plainAuth(), null);

        // when / then
        assertThatThrownBy(() -> manager.apply(broken))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing-role");
        assertThat(virtualTopics.toPhysical("orders")).isEqualTo("orders-v2");
        assertThat(virtualTopics.toPhysical("customers")).isEqualTo("customers");
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
        assertThat(manager.getActiveConfig()).isSameAs(good);
    }

    @Test
    void lastConfigIsNullBeforeFirstSnapshot() {
        // given
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance());

        // when / then
        // An empty config topic on first boot is valid: no snapshot has been applied, so the
        // manager reports no current config and the consumers stay at their initial empty state.
        assertThat(manager.getActiveConfig()).isNull();
    }

    @Test
    void readsItsOwnCommittedSnapshotBeforeTheConsumerAppliesIt() {
        // given
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        var committed = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth(), null);

        // when
        manager.update(_ -> committed);

        // then - the admin API reads its own write although the consumer has not applied it yet
        assertThat(manager.getActiveConfig()).isSameAs(committed);
    }

    @Test
    void updateBuildsOnLastAppliedConfigBeforeFirstPersist() {
        // given
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        var applied = config(Map.of("orders", new VirtualTopicConfig("orders-v2")),
                rbacAllowingReadOnOrders(), plainAuth(), null);
        manager.apply(applied);

        // when - a mutation is applied before anything has been committed through the writer
        manager.update(c -> c.upsertVirtualTopic("customers", new VirtualTopicConfig("crm.customers")));

        // then - the base is the last applied config, not an empty one
        assertThat(writer.committed.getLast().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"))
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
    }

    @Test
    void waitsUntilEveryCommittedSnapshotIsConsumedBeforeBuildingOnIt() throws Exception {
        // given - another replica committed a snapshot this replica's consumer has not read yet
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        var otherReplicas = GatewayConfig.empty().upsertVirtualTopic("payments", new VirtualTopicConfig("payments-v1"));
        long otherOffset = writer.commitByAnotherReplica(otherReplicas);
        Thread consumer = new Thread(() -> {
            sleep(50);
            manager.apply(otherReplicas, otherOffset);
        });
        consumer.start();

        // when
        manager.update(c -> c.upsertVirtualTopic("customers", new VirtualTopicConfig("crm.customers")));

        // then - the write built on the other replica's snapshot instead of overwriting it
        consumer.join();
        assertThat(writer.committed.getLast().virtualTopics())
                .withFailMessage(() -> "the write lost the other replica's change: " + writer.committed.getLast().virtualTopics())
                .containsKeys("payments", "customers");
    }

    @Test
    void waitsForItsOwnLastCommitBeforeTheNextWriteEvenWhenTheEndOffsetLags() {
        // given - a commit returns before its transaction marker reaches the topic, so the committed
        // end offset still excludes it; the consumer reads it a little later
        var writer = new FakeConfigTopicWriter();
        writer.lagEndOffsetBehindOwnCommits();
        var manager = manager(writer, Duration.ofSeconds(1));
        manager.update(c -> c.upsertVirtualTopic("customers", new VirtualTopicConfig("crm.customers")));
        var first = writer.committed.getLast();
        Thread consumer = new Thread(() -> {
            sleep(50);
            manager.apply(first, 0);
        });
        consumer.start();

        // when - the same replica writes again straight away
        manager.update(c -> c.upsertVirtualTopic("payments", new VirtualTopicConfig("payments-v1")));

        // then - the second write built on the first instead of dropping it
        assertThat(writer.committed.getLast().virtualTopics())
                .withFailMessage(() -> "the second write lost the first: " + writer.committed.getLast().virtualTopics())
                .containsKeys("customers", "payments");
    }

    @Test
    void retriesAFencedWriteOnTheNewerSnapshot() {
        // given - another replica commits between this replica's read and its commit
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        var otherReplicas = GatewayConfig.empty().upsertVirtualTopic("payments", new VirtualTopicConfig("payments-v1"));
        writer.fenceNextCommit(() -> manager.apply(otherReplicas, writer.commitByAnotherReplica(otherReplicas)));
        var bases = new ArrayList<GatewayConfig>();

        // when
        manager.update(c -> {
            bases.add(c);
            return c.upsertVirtualTopic("customers", new VirtualTopicConfig("crm.customers"));
        });

        // then
        assertThat(bases)
                .withFailMessage(() -> "the fenced write was not retried on the newer snapshot: " + bases)
                .hasSize(2);
        assertThat(writer.committed.getLast().virtualTopics()).containsKeys("payments", "customers");
    }

    @Test
    void reRunsTheMutationsChecksOnTheNewerSnapshot() {
        // given - the mutation only allows removing 'orders' while 'payments' remains; another
        // replica removes 'payments' while this write is in flight
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        var both = GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1"))
                .upsertVirtualTopic("payments", new VirtualTopicConfig("payments-v1"));
        manager.apply(both, writer.commitByAnotherReplica(both));
        var withoutPayments = both.removeVirtualTopic("payments");
        writer.fenceNextCommit(() -> manager.apply(withoutPayments, writer.commitByAnotherReplica(withoutPayments)));

        // when / then
        assertThatThrownBy(() -> manager.update(c -> {
            if (!c.virtualTopics().containsKey("payments")) {
                throw new IllegalArgumentException("'orders' is the last virtual topic");
            }
            return c.removeVirtualTopic("orders");
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'orders' is the last virtual topic");
        assertThat(writer.committed.getLast())
                .withFailMessage(() -> "a write whose check failed on the newer snapshot was committed")
                .isSameAs(withoutPayments);
    }

    @Test
    void givesUpWhenEveryAttemptIsFenced() {
        // given
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        writer.fenceEveryCommit();

        // when / then
        assertThatThrownBy(() -> manager.update(c -> c))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("kept conflicting with other gateway replicas");
    }

    @Test
    void closeIsSafeWithoutStart() {
        // given
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance());

        // when / then
        manager.close();
    }

    @Test
    void appliesGovernanceConfigToPolicy() {
        // given
        var governancePolicy = new GovernancePolicy(new GovernanceConfig(null));
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                governancePolicy);
        var governance = new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "must have partitions", "topic.partitions >= 1")));

        // when
        manager.apply(config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(), governance));

        // then
        assertThat(governancePolicy.evaluate("alice", "payments", new TopicSpec("orders", 0, 3, Map.of())))
                .extracting(Violation::rule)
                .containsExactly("min-partitions");
        assertThat(governancePolicy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()))).isEmpty();
    }

    @Test
    void rejectedGovernanceSnapshotLeavesPreviousStateIntact() {
        // given
        var governancePolicy = new GovernancePolicy(new GovernanceConfig(null));
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                governancePolicy);
        var good = config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(),
                new GovernanceConfig(Map.of(
                        "min-partitions", rule("min-partitions", "must have partitions", "topic.partitions >= 1"))));
        manager.apply(good);
        var broken = config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(),
                new GovernanceConfig(Map.of(
                        "broken", rule("broken", "broken rule", "topic.partitions >="))));

        // when / then
        assertThatThrownBy(() -> manager.apply(broken))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("broken");
        assertThat(governancePolicy.evaluate("alice", "payments", new TopicSpec("orders", 0, 3, Map.of())))
                .extracting(Violation::rule)
                .containsExactly("min-partitions");
    }

    @Test
    void appliesRuleExemptionsToPolicy() {
        // given
        var governancePolicy = new GovernancePolicy(new GovernanceConfig(null));
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                governancePolicy);
        var governance = new GovernanceConfig(Map.of("min-partitions", new GovernanceRuleConfig(
                "min-partitions",
                "must have partitions",
                "must have partitions",
                Selector.topic(Expression.cel("true")),
                Expression.cel("topic.partitions >= 1"),
                List.of(new GovernanceRuleConfig.Exemption(
                        "streams-internal",
                        "Kafka Streams manages its own changelog topics.",
                        Expression.cel("principal.startsWith('streams-') && topic.name.endsWith('-changelog')"))))));

        // when
        manager.apply(config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(), governance));

        // then
        assertThat(governancePolicy.evaluate("streams-app", "payments", new TopicSpec("orders-changelog", 0, 3, Map.of())))
                .isEmpty();
        assertThat(governancePolicy.evaluate("other-app", "payments", new TopicSpec("orders-changelog", 0, 3, Map.of())))
                .extracting(Violation::rule)
                .containsExactly("min-partitions");
    }

    @Test
    void updateAndWaitUntilAppliedReturnsAfterWrittenOffsetIsApplied() {
        // given
        var writer = new FakeConfigTopicWriter();
        var manager = manager(writer, Duration.ofSeconds(1));
        var targetConfig = config(
                Map.of("orders", new VirtualTopicConfig("orders-v2")),
                rbacAllowingReadOnOrders(),
                plainAuth(),
                null);
        long writtenOffset = writer.endOffset;
        Thread applier = new Thread(() -> {
            sleep(50);
            manager.apply(targetConfig, writtenOffset);
        });
        applier.start();

        // when
        manager.updateAndWaitUntilApplied(_ -> targetConfig);

        // then
        assertThat(manager.getActiveConfig()).isSameAs(targetConfig);
    }

    @Test
    void updateAndWaitUntilAppliedTimesOutWhenOffsetIsNotApplied() {
        // given
        var manager = manager(new FakeConfigTopicWriter(), Duration.ofMillis(20));

        // when / then
        assertThatThrownBy(() -> manager.updateAndWaitUntilApplied(_ -> GatewayConfig.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timed out waiting for config apply")
                .hasMessageContaining("targetOffset=");
    }

    @Test
    void updateTimesOutWhenTheConsumerNeverCatchesUp() {
        // given - a committed snapshot this replica's consumer never reads
        var writer = new FakeConfigTopicWriter();
        writer.commitByAnotherReplica(GatewayConfig.empty());
        var manager = manager(writer, Duration.ofMillis(20));

        // when / then
        assertThatThrownBy(() -> manager.update(c -> c))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timed out waiting for the config consumer to read up to offset 1");
    }

    private DynamicConfigManager manager(ConfigTopicWriter writer, Duration applyWaitTimeout) {
        return new DynamicConfigManager(writer,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance(),
                applyWaitTimeout);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /// An in-memory config topic shared with simulated other replicas: commits get consecutive
    /// offsets, and a commit can be fenced after running what another replica did meanwhile.
    private static final class FakeConfigTopicWriter implements ConfigTopicWriter {

        private final List<GatewayConfig> committed = new ArrayList<>();
        private long endOffset;
        private final Deque<Runnable> fences = new ArrayDeque<>();
        private boolean fenceAlways;
        /// When set, this writer's own commits never move [#committedEndOffset], like a commit whose
        /// transaction marker has not reached the topic yet.
        private boolean lagOwnCommits;
        private long visibleEnd;

        /// A snapshot another replica committed; returns its offset.
        long commitByAnotherReplica(GatewayConfig config) {
            committed.add(config);
            visibleEnd = endOffset + 1;
            return endOffset++;
        }

        /// The next commit runs `otherReplica` and is then fenced.
        void fenceNextCommit(Runnable otherReplica) {
            fences.add(otherReplica);
        }

        void fenceEveryCommit() {
            fenceAlways = true;
        }

        void lagEndOffsetBehindOwnCommits() {
            lagOwnCommits = true;
        }

        @Override
        public long commit(GatewayConfig config) {
            if (fenceAlways || !fences.isEmpty()) {
                if (!fences.isEmpty()) {
                    fences.poll().run();
                }
                throw new ConfigWriteFencedException("another writer took over", new RuntimeException());
            }
            committed.add(config);
            if (!lagOwnCommits) {
                visibleEnd = endOffset + 1;
            }
            return endOffset++;
        }

        @Override
        public long committedEndOffset() {
            return visibleEnd;
        }

        @Override
        public void close() {
            // no resources
        }
    }
}
