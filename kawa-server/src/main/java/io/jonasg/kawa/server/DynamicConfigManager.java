package io.jonasg.kawa.server;

import io.jonasg.kawa.config.ConfigTopicConsumer;
import io.jonasg.kawa.config.ConfigTopicRepository;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.OffsetAwareGatewayConfigRepository;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/// Owns the config-topic consumer and applies each [GatewayConfig] snapshot to the mutable
/// consumers: [VirtualTopicManager], [RbacAuthorizer], [SaslAuthenticator] and
/// [GovernancePolicy]. Also owns the config-topic producer, so it is the
/// [GatewayConfigRepository] the admin HTTP surface uses to read the current snapshot and
/// persist changes.
///
/// A snapshot is applied in an order that keeps the application atomic in the failure case:
/// [RbacAuthorizer#reload] and [GovernancePolicy#reload] are the consumers that can reject a
/// snapshot (an unknown role reference or an invalid CEL expression throws), so they run
/// first. Each of them is atomic on its own - a failed reload keeps the previous state - and
/// the non-risky consumers ([VirtualTopicManager], [SaslAuthenticator]) are only touched
/// after both risky reloads have succeeded.
///
/// [awaitInitialLoad] blocks until the config topic has been caught up from the earliest
/// offset, so the gateway can refuse to serve until it has applied the config that existed
/// at boot.
///
/// [current] reports the newest *persisted* snapshot (from the write repository) when one
/// exists, falling back to the last *applied* one. The admin API's read-modify-write must
/// build on the persisted snapshot: the consumer applies asynchronously, so a burst of PUTs
/// would otherwise each start from the same stale base and overwrite each other.
public final class DynamicConfigManager implements GatewayConfigRepository, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DynamicConfigManager.class);

    private static final Duration DEFAULT_APPLY_WAIT_TIMEOUT = Duration.ofSeconds(5);

    private final ConfigTopicConsumer consumer;
    private final GatewayConfigRepository writeRepository;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;
    private final GovernancePolicy governance;
    private final Duration applyWaitTimeout;
    private final Object applyProgressLock = new Object();

    private volatile GatewayConfig current;
    private volatile long lastAppliedOffset = -1L;

    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this(bootstrapServers, topic, new Properties(), virtualTopics, authorizer, saslAuthenticator,
                governance, DEFAULT_APPLY_WAIT_TIMEOUT);
    }

    /// Variant that accepts extra consumer/producer properties (e.g. SASL/security settings
    /// for the config topic) on top of the base bootstrap/deserializer configuration.
    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            Properties extraProps,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this(bootstrapServers, topic, extraProps, virtualTopics, authorizer, saslAuthenticator, governance,
                DEFAULT_APPLY_WAIT_TIMEOUT);
    }

    DynamicConfigManager(
            String bootstrapServers,
            String topic,
            Properties extraProps,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance,
            Duration applyWaitTimeout
    ) {
        this.virtualTopics = virtualTopics;
        this.authorizer = authorizer;
        this.saslAuthenticator = saslAuthenticator;
        this.governance = governance;
        this.applyWaitTimeout = applyWaitTimeout;
        this.consumer = new ConfigTopicConsumer(
                bootstrapServers,
                topic,
                extraProps,
                (config, offset) -> apply(config, offset));
        this.writeRepository = new ConfigTopicRepository(bootstrapServers, topic, extraProps);
    }

    /// Test seam: injects the write-side repository so the read-modify-write base can be
    /// verified without a broker. The consumer is created against a dummy bootstrap and never
    /// started.
    DynamicConfigManager(
            GatewayConfigRepository writeRepository,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this(writeRepository, virtualTopics, authorizer, saslAuthenticator, governance, DEFAULT_APPLY_WAIT_TIMEOUT);
    }

    DynamicConfigManager(
            GatewayConfigRepository writeRepository,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance,
            Duration applyWaitTimeout
    ) {
        this.virtualTopics = virtualTopics;
        this.authorizer = authorizer;
        this.saslAuthenticator = saslAuthenticator;
        this.governance = governance;
        this.applyWaitTimeout = applyWaitTimeout;
        this.consumer = new ConfigTopicConsumer(
                "localhost:9092",
                "__kawa",
                new Properties(),
                (config, offset) -> apply(config, offset));
        this.writeRepository = writeRepository;
    }

    /// Applies a snapshot to the four mutable consumers. Package-private so the wiring is
    /// testable without a broker.
    void apply(GatewayConfig config) {
        applyConsumers(config);
        current = config;
    }

    /// Applies a snapshot and advances the last-applied offset used by
    /// [updateAndWaitUntilApplied]. Package-private so offset-wait behavior is testable.
    void apply(GatewayConfig config, long offset) {
        applyConsumers(config);
        current = config;
        synchronized (applyProgressLock) {
            lastAppliedOffset = Math.max(lastAppliedOffset, offset);
            applyProgressLock.notifyAll();
        }
    }

    private void applyConsumers(GatewayConfig config) {
        authorizer.reload(config.rbac()); // risky first: can throw on unknown role
        governance.reload(config.governance()); // risky: can throw on invalid CEL expression
        virtualTopics.reload(config.virtualTopics());
        saslAuthenticator.reload(config.auth().mechanisms(), config.auth().clients());
    }

    /// Starts the config-topic consumer. Idempotent.
    public void start() {
        consumer.start();
    }

    /// Blocks until the config topic has been caught up from the earliest offset.
    public void awaitInitialLoad() throws InterruptedException {
        consumer.awaitInitialLoad();
    }

    @Override
    public GatewayConfig getActiveConfig() {
        // The admin API's read-modify-write must build on the most recent *persisted*
        // snapshot, not the last *applied* one: the consumer applies asynchronously, so a
        // burst of PUTs would otherwise each start from the same stale base and overwrite
        // each other. The write repository tracks the newest persisted snapshot.
        GatewayConfig persisted = writeRepository.getActiveConfig();
        return persisted != null ? persisted : current;
    }

    /// Applies [mutation] to the active config (persisted-or-applied, or empty before the
    /// first snapshot) and persists the result.
    @Override
    public void update(UnaryOperator<GatewayConfig> mutation) {
        // The write repository only knows persisted snapshots, so the read-modify-write base
        // must be resolved here: before the first persist it falls back to the last applied
        // config (see getActiveConfig).
        GatewayConfig base = getActiveConfigOrEmpty();
        writeRepository.update(ignored -> mutation.apply(base));
    }

    @Override
    public void updateAndWaitUntilApplied(UnaryOperator<GatewayConfig> mutation) {
        GatewayConfig base = getActiveConfigOrEmpty();
        if (!(writeRepository instanceof OffsetAwareGatewayConfigRepository offsetAwareRepository)) {
            writeRepository.updateAndWaitUntilApplied(ignored -> mutation.apply(base));
            return;
        }
        long writtenOffset = offsetAwareRepository.updateAndGetOffset(ignored -> mutation.apply(base));
        awaitAppliedOffset(writtenOffset);
    }

    private void awaitAppliedOffset(long targetOffset) {
        long timeoutNanos = applyWaitTimeout.toNanos();
        long deadline = System.nanoTime() + timeoutNanos;
        synchronized (applyProgressLock) {
            while (lastAppliedOffset < targetOffset) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new IllegalStateException(
                            "timed out waiting for config apply after persist; targetOffset=" + targetOffset
                            + ", lastAppliedOffset=" + lastAppliedOffset
                            + ", timeout=" + applyWaitTimeout.toMillis() + "ms");
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(applyProgressLock, remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "interrupted while waiting for config apply at offset " + targetOffset, e);
                }
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
        try {
            writeRepository.close();
        } catch (Exception e) {
            log.warn("failed to close config write repository", e);
        }
    }
}
