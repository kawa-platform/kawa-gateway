package io.jonasg.kawa.server;

import io.jonasg.kawa.config.ConfigTopicConsumer;
import io.jonasg.kawa.config.ConfigTopicWriter;
import io.jonasg.kawa.config.ConfigWriteFencedException;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.TransactionalConfigTopicWriter;
import io.jonasg.kawa.core.module.ModuleRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Owns the config-topic consumer and applies each [GatewayConfig] snapshot to the mutable
/// consumers: [VirtualTopicManager], [RbacAuthorizer], [SaslAuthenticator], [GovernancePolicy]
/// and [ModuleRegistry]. Also owns the config-topic writer, so it is the
/// [GatewayConfigRepository] the admin HTTP surface uses to read the current snapshot and
/// persist changes.
///
/// A snapshot is applied in an order that keeps the application atomic in the failure case:
/// [RbacAuthorizer#reload] and [GovernancePolicy#reload] are the consumers that can reject a
/// snapshot (an unknown role reference or an invalid CEL expression throws), so they run
/// first. Each of them is atomic on its own - a failed reload keeps the previous state - and
/// the non-risky consumers ([VirtualTopicManager], [SaslAuthenticator]) are only touched
/// after both risky reloads have succeeded. The [ModuleRegistry] goes last, so a rejected snapshot
/// never switches a module on or off.
///
/// [awaitInitialLoad] blocks until the config topic has been caught up from the earliest
/// offset, so the gateway can refuse to serve until it has applied the config that existed
/// at boot.
///
/// Every write is a compare-and-set across gateway replicas: the [ConfigTopicWriter] takes over
/// the config topic (fencing every other replica's writer), this manager waits until its consumer
/// has read every committed snapshot, applies the mutation to that snapshot and commits. When
/// another replica took over in between, the commit is fenced and the write starts again on the
/// newer snapshot, so a mutation always sees every change committed before its own and its checks
/// (such as "not the last enabled admin user") hold on what is written. Writes on one replica are
/// serialized. A mutation may therefore run more than once and must be a pure function of its base.
///
/// [getActiveConfig] reports this replica's own newest committed snapshot until the consumer has
/// applied it, then the last applied one, so the admin API reads its own writes.
public final class DynamicConfigManager implements GatewayConfigRepository, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DynamicConfigManager.class);

    private static final Duration DEFAULT_APPLY_WAIT_TIMEOUT = Duration.ofSeconds(5);
    private static final int MAX_WRITE_ATTEMPTS = 10;

    private final ConfigTopicConsumer consumer;
    private final ConfigTopicWriter writer;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;
    private final GovernancePolicy governance;
    private final ModuleRegistry modules;
    private final Duration applyWaitTimeout;
    private final Object applyProgressLock = new Object();
    private final Object writeLock = new Object();

    private volatile @Nullable GatewayConfig current;
    private volatile long lastAppliedOffset = -1L;
    private volatile long consumedPosition = 0L;
    private volatile @Nullable Committed lastCommitted;

    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this(bootstrapServers, topic, new Properties(), virtualTopics, authorizer, saslAuthenticator, governance,
                new ModuleRegistry());
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
            GovernancePolicy governance,
            ModuleRegistry modules
    ) {
        this(new TransactionalConfigTopicWriter(bootstrapServers, topic, extraProps), bootstrapServers, topic, extraProps,
                virtualTopics, authorizer, saslAuthenticator, governance, modules, DEFAULT_APPLY_WAIT_TIMEOUT);
    }

    /// Test seam: injects the writer so the compare-and-set flow can be verified without a broker.
    /// The consumer is created against a dummy bootstrap and never started; tests drive [apply].
    DynamicConfigManager(
            ConfigTopicWriter writer,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance,
            Duration applyWaitTimeout
    ) {
        this(writer, virtualTopics, authorizer, saslAuthenticator, governance, new ModuleRegistry(), applyWaitTimeout);
    }

    /// Test seam with the [ModuleRegistry] to reload.
    DynamicConfigManager(
            ConfigTopicWriter writer,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance,
            ModuleRegistry modules,
            Duration applyWaitTimeout
    ) {
        this(writer, "localhost:9092", "__kawa", new Properties(), virtualTopics, authorizer, saslAuthenticator,
                governance, modules, applyWaitTimeout);
    }

    private DynamicConfigManager(
            ConfigTopicWriter writer,
            String bootstrapServers,
            String topic,
            Properties extraProps,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance,
            ModuleRegistry modules,
            Duration applyWaitTimeout
    ) {
        this.writer = writer;
        this.virtualTopics = virtualTopics;
        this.authorizer = authorizer;
        this.saslAuthenticator = saslAuthenticator;
        this.governance = governance;
        this.modules = modules;
        this.applyWaitTimeout = applyWaitTimeout;
        this.consumer = new ConfigTopicConsumer(
                bootstrapServers,
                topic,
                extraProps,
                this::apply,
                this::consumedUpTo);
    }

    /// Applies a snapshot to the four mutable consumers. Package-private so the wiring is
    /// testable without a broker.
    void apply(GatewayConfig config) {
        applyConsumers(config);
        current = config;
    }

    /// Applies the snapshot at `offset` and advances the progress [updateAndWaitUntilApplied] and
    /// the compare-and-set wait on. Package-private so the write flow is testable.
    void apply(GatewayConfig config, long offset) {
        applyConsumers(config);
        current = config;
        synchronized (applyProgressLock) {
            lastAppliedOffset = Math.max(lastAppliedOffset, offset);
            consumedPosition = Math.max(consumedPosition, offset + 1);
            applyProgressLock.notifyAll();
        }
    }

    /// The consumer has read everything before `position`, including aborted records and
    /// transaction markers that carry no snapshot.
    private void consumedUpTo(long position) {
        synchronized (applyProgressLock) {
            consumedPosition = Math.max(consumedPosition, position);
            applyProgressLock.notifyAll();
        }
    }

    private void applyConsumers(GatewayConfig config) {
        authorizer.reload(config.rbac()); // risky first: can throw on unknown role
        governance.reload(config.governance()); // risky: can throw on invalid CEL expression
        virtualTopics.reload(config.virtualTopics());
        saslAuthenticator.reload(config.auth().mechanisms(), config.auth().clients());
        modules.reload(config.modules());
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
    public @Nullable GatewayConfig getActiveConfig() {
        Committed committed = lastCommitted;
        if (committed != null && committed.offset() > lastAppliedOffset) {
            return committed.config();
        }
        return current;
    }

    @Override
    public void update(UnaryOperator<GatewayConfig> mutation) {
        commit(mutation);
    }

    @Override
    public void updateAndWaitUntilApplied(UnaryOperator<GatewayConfig> mutation) {
        awaitAppliedOffset(commit(mutation));
    }

    /// Applies `mutation` to the newest committed snapshot and commits the result, starting again on
    /// a newer snapshot when another replica wrote in between. Returns the offset of the commit.
    private long commit(UnaryOperator<GatewayConfig> mutation) {
        synchronized (writeLock) {
            for (int attempt = 1; ; attempt++) {
                // The committed end offset can trail this replica's own last commit: committing
                // returns once the transaction log records it, before its marker reaches the config
                // topic. Waiting for the consumer to read past our own last commit as well keeps a
                // write from building on a snapshot that is missing the previous one.
                Committed own = lastCommitted;
                awaitConsumedPosition(Math.max(writer.committedEndOffset(), own == null ? 0 : own.offset() + 1));
                GatewayConfig base = current;
                GatewayConfig next = mutation.apply(base != null ? base : GatewayConfig.empty());
                try {
                    long offset = writer.commit(next);
                    lastCommitted = new Committed(next, offset);
                    return offset;
                } catch (ConfigWriteFencedException e) {
                    if (attempt == MAX_WRITE_ATTEMPTS) {
                        throw new IllegalStateException("config write kept conflicting with other gateway replicas; "
                                + "gave up after " + attempt + " attempts", e);
                    }
                    log.info("config write conflicted with another gateway replica; retrying on the newer snapshot "
                            + "(attempt {} of {})", attempt, MAX_WRITE_ATTEMPTS);
                    backOff(attempt);
                }
            }
        }
    }

    private static void backOff(int attempt) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(5, 25) * attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while retrying a config write", e);
        }
    }

    /// Blocks until the consumer has read every record before `position`.
    private void awaitConsumedPosition(long position) {
        awaitProgress(() -> consumedPosition >= position,
                () -> "timed out waiting for the config consumer to read up to offset " + position
                        + "; consumedPosition=" + consumedPosition + ", timeout=" + applyWaitTimeout.toMillis() + "ms");
    }

    private void awaitAppliedOffset(long targetOffset) {
        awaitProgress(() -> lastAppliedOffset >= targetOffset,
                () -> "timed out waiting for config apply after persist; targetOffset=" + targetOffset
                        + ", lastAppliedOffset=" + lastAppliedOffset + ", timeout=" + applyWaitTimeout.toMillis() + "ms");
    }

    private void awaitProgress(BooleanSupplier reached, Supplier<String> timeoutMessage) {
        long deadline = System.nanoTime() + applyWaitTimeout.toNanos();
        synchronized (applyProgressLock) {
            while (!reached.getAsBoolean()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new IllegalStateException(timeoutMessage.get());
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(applyProgressLock, remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for the config consumer", e);
                }
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
        try {
            writer.close();
        } catch (Exception e) {
            log.warn("failed to close config writer", e);
        }
    }

    /// A snapshot this replica committed, and the offset of its record.
    private record Committed(GatewayConfig config, long offset) {
    }
}
