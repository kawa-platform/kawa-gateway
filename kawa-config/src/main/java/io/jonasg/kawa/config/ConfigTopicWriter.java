package io.jonasg.kawa.config;

/// Writes [GatewayConfig] snapshots to the config topic as a compare-and-set.
///
/// Every gateway replica's writer shares one Kafka `transactional.id`, so at most one of them can
/// write at a time: a writer that starts writing fences the others, and a fenced writer's commit
/// fails with [ConfigWriteFencedException]. A write therefore only succeeds when no other writer
/// committed since this writer took over, so a snapshot built on the config topic's state read
/// *after* taking over (up to [#committedEndOffset]) never overwrites a change it did not see.
public interface ConfigTopicWriter extends AutoCloseable {

    /// Commits `config` as the newest snapshot and returns the offset of its record.
    ///
    /// @throws ConfigWriteFencedException when another writer committed or started writing since this
    ///                                    writer last took over; nothing was written
    long commit(GatewayConfig config);

    /// The offset just past the last committed record on the config topic: a consumer that has read
    /// up to here has seen every committed snapshot. Taking over first, so a commit by another
    /// writer after this call fences this writer.
    long committedEndOffset();

    /// Releases the writer's resources.
    @Override
    void close();
}
