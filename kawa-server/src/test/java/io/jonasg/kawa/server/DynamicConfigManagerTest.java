package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.OffsetAwareGatewayConfigRepository;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

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

    private static GovernancePolicy emptyGovernance() {
        return new GovernancePolicy(new GovernanceConfig(null, null));
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
    void currentReturnsLastPersistedSnapshotBeforeConsumerAppliesIt() {
        // given - a manager whose write side is an in-memory repository (no broker needed)
        var writeRepository = new GatewayConfigRepository() {
            private GatewayConfig last;

            @Override
            public GatewayConfig getActiveConfig() {
                return last;
            }

            private void upsert(GatewayConfig config) {
                last = config;
            }

            @Override
            public void update(UnaryOperator<GatewayConfig> mutation) {
                upsert(mutation.apply(getActiveConfigOrEmpty()));
            }

            @Override
            public void updateAndWaitUntilApplied(UnaryOperator<GatewayConfig> mutation) {
                update(mutation);
            }

            @Override
            public void close() {
                // no resources
            }
        };
        var manager = new DynamicConfigManager(writeRepository,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance());
        var first = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth(), null);
        var second = config(Map.of("customers", new VirtualTopicConfig("crm.customers")), rbacAllowingReadOnOrders(), plainAuth(), null);

        // when - two snapshots are persisted before the consumer has applied either
        manager.update(_ -> first);
        manager.update(_ -> second);

        // then - the read-modify-write base is the newest persisted snapshot, so a burst of
        // PUTs builds on each other instead of overwriting from the same stale base
        assertThat(manager.getActiveConfig()).isSameAs(second);
    }

    @Test
    void updateBuildsOnLastAppliedConfigBeforeFirstPersist() {
        // given - a manager whose write side is an in-memory repository (no broker needed)
        var writeRepository = new GatewayConfigRepository() {
            private GatewayConfig last;

            @Override
            public GatewayConfig getActiveConfig() {
                return last;
            }

            private void upsert(GatewayConfig config) {
                last = config;
            }

            @Override
            public void update(UnaryOperator<GatewayConfig> mutation) {
                upsert(mutation.apply(getActiveConfigOrEmpty()));
            }

            @Override
            public void updateAndWaitUntilApplied(UnaryOperator<GatewayConfig> mutation) {
                update(mutation);
            }

            @Override
            public void close() {
                // no resources
            }
        };
        var manager = new DynamicConfigManager(writeRepository,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance());
        var applied = config(Map.of("orders", new VirtualTopicConfig("orders-v2")),
                rbacAllowingReadOnOrders(), plainAuth(), null);
        manager.apply(applied);

        // when - a mutation is applied before anything has been persisted through the write side
        manager.update(c -> c.upsertVirtualTopic("customers", new VirtualTopicConfig("crm.customers")));

        // then - the base is the last applied config, not an empty one
        assertThat(manager.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"))
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
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
        var governancePolicy = new GovernancePolicy(new GovernanceConfig(null, null));
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                governancePolicy);
        var governance = new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")), null);

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
        var governancePolicy = new GovernancePolicy(new GovernanceConfig(null, null));
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                governancePolicy);
        var good = config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(),
                new GovernanceConfig(Map.of(
                        "min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")), null));
        manager.apply(good);
        var broken = config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(),
                new GovernanceConfig(Map.of(
                        "broken", new GovernanceRuleConfig("broken rule", "topic.partitions >=")), null));

        // when / then
        assertThatThrownBy(() -> manager.apply(broken))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("broken");
        assertThat(governancePolicy.evaluate("alice", "payments", new TopicSpec("orders", 0, 3, Map.of())))
                .extracting(Violation::rule)
                .containsExactly("min-partitions");
    }

    @Test
    void appliesExemptionsToPolicy() {
        // given
        var governancePolicy = new GovernancePolicy(new GovernanceConfig(null, null));
        var manager = new DynamicConfigManager("localhost:9092", "__kawa",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                governancePolicy);
        var governance = new GovernanceConfig(null, Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        manager.apply(config(Map.of(), new RbacConfig(Map.of(), Map.of()), plainAuth(), governance));

        // then
        assertThat(governancePolicy.exempt("streams-app", "orders-changelog")).isTrue();
        assertThat(governancePolicy.exempt("other-app", "orders-changelog")).isFalse();
    }

    @Test
    void updateAndWaitUntilAppliedReturnsAfterWrittenOffsetIsApplied() {
        // given
        var writeRepository = new OffsetAwareTestRepository();
        var manager = new DynamicConfigManager(
                writeRepository,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance(),
                Duration.ofSeconds(1));
        var targetConfig = config(
                Map.of("orders", new VirtualTopicConfig("orders-v2")),
                rbacAllowingReadOnOrders(),
                plainAuth(),
                null);
        long writtenOffset = writeRepository.nextOffset;
        Thread applier = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            manager.apply(targetConfig, writtenOffset);
        });
        applier.start();

        // when
        manager.updateAndWaitUntilApplied(_ -> targetConfig);

        // then
        assertThat(writeRepository.updateAndGetOffsetCalls).isEqualTo(1);
        assertThat(manager.getActiveConfig()).isSameAs(targetConfig);
    }

    @Test
    void updateAndWaitUntilAppliedTimesOutWhenOffsetIsNotApplied() {
        // given
        var writeRepository = new OffsetAwareTestRepository();
        var manager = new DynamicConfigManager(
                writeRepository,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance(),
                Duration.ofMillis(20));

        // when / then
        assertThatThrownBy(() -> manager.updateAndWaitUntilApplied(_ -> GatewayConfig.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timed out waiting for config apply")
                .hasMessageContaining("targetOffset=");
    }

    @Test
    void updateAndWaitUntilAppliedFallsBackToRepositoryWhenOffsetIsUnavailable() {
        // given
        var writeRepository = new GatewayConfigRepository() {
            private GatewayConfig current;
            private int updateAndWaitCalls;

            @Override
            public GatewayConfig getActiveConfig() {
                return current;
            }

            @Override
            public void update(UnaryOperator<GatewayConfig> mutation) {
                current = mutation.apply(getActiveConfigOrEmpty());
            }

            @Override
            public void updateAndWaitUntilApplied(UnaryOperator<GatewayConfig> mutation) {
                updateAndWaitCalls++;
                current = mutation.apply(getActiveConfigOrEmpty());
            }

            @Override
            public void close() {
                // no resources
            }
        };
        var manager = new DynamicConfigManager(
                writeRepository,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(),
                emptyGovernance(),
                Duration.ofMillis(20));

        // when
        manager.updateAndWaitUntilApplied(_ -> GatewayConfig.empty().upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));

        // then
        assertThat(writeRepository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    private static final class OffsetAwareTestRepository implements OffsetAwareGatewayConfigRepository {
        private GatewayConfig current;
        private long nextOffset = 7;
        private int updateAndGetOffsetCalls;

        @Override
        public GatewayConfig getActiveConfig() {
            return current;
        }

        @Override
        public void update(UnaryOperator<GatewayConfig> mutation) {
            current = mutation.apply(getActiveConfigOrEmpty());
        }

        @Override
        public void updateAndWaitUntilApplied(UnaryOperator<GatewayConfig> mutation) {
            update(mutation);
        }

        @Override
        public long updateAndGetOffset(UnaryOperator<GatewayConfig> mutation) {
            updateAndGetOffsetCalls++;
            current = mutation.apply(getActiveConfigOrEmpty());
            return nextOffset++;
        }

        @Override
        public void close() {
            // no resources
        }
    }
}
