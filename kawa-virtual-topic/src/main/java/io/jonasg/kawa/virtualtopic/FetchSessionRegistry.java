package io.jonasg.kawa.virtualtopic;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/// Tracks the virtual/physical topic mapping of active fetch sessions (KIP-227, Fetch v7+), and the
/// offset the client last fetched each partition at, which an incremental fetch leaves out while it
/// does not change.
///
/// Incremental session fetches omit the topics list once a partition is caught up, so the
/// per-request [VirtualTopicState] is empty and the response transform has nothing to rename
/// response topics against. This registry remembers the mapping per (client, broker-assigned
/// session id) so idle incremental responses can still be renamed back to virtual names.
///
/// Entries are bound when a session is created (full fetch response) and updated while it is
/// alive. They are dropped when the client closes the session (final epoch), when the broker
/// reports the session as unknown, when the client disconnects, or after an idle TTL.
public final class FetchSessionRegistry {

    private static final long TTL_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int MAX_SESSIONS = 4096;

    private static final class Entry {

        private final Map<String, String> physicalToVirtual = new HashMap<>();
        private final Map<TopicPartition, Long> fetchOffsets = new ConcurrentHashMap<>();
        private volatile long lastSeenNanos;

        private Entry(long nowNanos) {
            this.lastSeenNanos = nowNanos;
        }
    }

    private record Key(Object client, int sessionId) {
    }

    private final Map<Key, Entry> sessions = new ConcurrentHashMap<>();

    /// Applies a fetch request to an existing session: merges newly renamed topics, forgets
    /// the given physical topics and records activity. No-op for full fetches (session not yet
    /// created) or unknown sessions.
    public void onFetchRequest(Object client, int sessionId, Map<String, String> physicalToVirtual,
                               List<String> forgottenPhysical) {
        onFetchRequest(client, sessionId, physicalToVirtual, forgottenPhysical, Map.of(), List.of());
    }

    /// As [#onFetchRequest(Object, int, Map, List)], also remembering the fetch offset of every
    /// partition the request names and forgetting the offsets of the partitions it forgets. A
    /// partition an incremental fetch leaves out keeps its offset: the client only names a partition
    /// when its fetch offset changed.
    public void onFetchRequest(Object client, int sessionId, Map<String, String> physicalToVirtual, List<String> forgottenPhysical,
                               Map<TopicPartition, Long> fetchOffsets, List<TopicPartition> forgottenPartitions) {
        if (sessionId == 0) {
            return;
        }
        long now = System.nanoTime();
        maybePrune(now);
        var key = new Key(client, sessionId);
        Entry entry = sessions.get(key);
        if (entry == null) {
            return;
        }
        entry.lastSeenNanos = now;
        entry.physicalToVirtual.putAll(physicalToVirtual);
        forgottenPhysical.forEach(entry.physicalToVirtual::remove);
        entry.fetchOffsets.putAll(fetchOffsets);
        forgottenPartitions.forEach(entry.fetchOffsets::remove);
    }

    /// Binds a freshly created session (full-fetch response) to the request's topic mapping.
    public void bindSession(
            Object client,
            int sessionId,
            Map<String, String> physicalToVirtual
    ) {
        bindSession(client, sessionId, physicalToVirtual, Map.of());
    }

    /// Binds a freshly created session to the request's topic mapping and the fetch offset of each
    /// partition it named.
    public void bindSession(
            Object client,
            int sessionId,
            Map<String, String> physicalToVirtual,
            Map<TopicPartition, Long> fetchOffsets
    ) {
        long now = System.nanoTime();
        maybePrune(now);
        if (sessionId == 0 || physicalToVirtual.isEmpty()) {
            return;
        }
        Entry entry = sessions.computeIfAbsent(new Key(client, sessionId), k -> new Entry(now));
        entry.physicalToVirtual.putAll(physicalToVirtual);
        entry.fetchOffsets.putAll(fetchOffsets);
    }

    /// The offset the client last fetched `partition` (physical topic) at on this session, if known.
    public OptionalLong fetchOffset(
            Object client,
            int sessionId,
            TopicPartition partition
    ) {
        Entry entry = sessions.get(new Key(client, sessionId));
        Long offset = entry == null ? null : entry.fetchOffsets.get(partition);
        return offset == null ? OptionalLong.empty() : OptionalLong.of(offset);
    }

    public boolean hasSession(
            Object client,
            int sessionId
    ) {
        return sessionId != 0 && sessions.containsKey(new Key(client, sessionId));
    }

    /// The virtual (client-visible) name for `physical` on this session, or `null`.
    public String virtualFor(
            Object client,
            int sessionId,
            String physical
    ) {
        long now = System.nanoTime();
        maybePrune(now);
        Entry entry = sessions.get(new Key(client, sessionId));
        if (entry == null) {
            return null;
        }
        entry.lastSeenNanos = now;
        return entry.physicalToVirtual.get(physical);
    }

    /// Drops one session (closed by the client or reported unknown by the broker).
    public void removeSession(
            Object client,
            int sessionId
    ) {
        sessions.remove(new Key(client, sessionId));
    }

    /// Drops every session belonging to a disconnected client.
    public void sessionClosed(Object client) {
        sessions.keySet().removeIf(key -> key.client() == client);
    }

    private void maybePrune(long now) {
        if (sessions.size() < MAX_SESSIONS) {
            return;
        }
        sessions.entrySet().removeIf(entry -> now - entry.getValue().lastSeenNanos > TTL_NANOS);
    }
}
