package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/// Maintains the virtual-to-physical virtual-topic map, along with each virtual topic's
/// optional consume filter configuration.
///
/// The whole mapping is an immutable [Snapshot] swapped atomically via a single `volatile`
/// reference: [reload] publishes a new snapshot in one write, so a reader on the hot path
/// observes either the previous or the new mapping in its entirety - never a mix of the two.
///
/// Topic names that are not virtualized map to themselves (identity) and carry no filter.
public final class VirtualTopicManager {

    private volatile Snapshot snapshot;

    public VirtualTopicManager(Map<String, VirtualTopicConfig> virtualTopics) {
        reload(virtualTopics);
    }

    /// Replaces the virtual-topic mapping with a new snapshot built from `virtualTopics`.
    ///
    /// Safe to call concurrently with readers: the new snapshot is assigned to a single
    /// `volatile` reference, so readers see either the previous or the new mapping, never a
    /// partially-applied one.
    public void reload(Map<String, VirtualTopicConfig> virtualTopics) {
        Map<String, VirtualTopic> virtual = new LinkedHashMap<>();
        Map<String, VirtualTopic> physical = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        virtualTopics.forEach((v, config) -> {
            VirtualTopic virtualTopic = new VirtualTopic(v, config.topic(), config.filter(), config.exposePhysicalTopic(), config.valueFormat());
            virtual.put(v, virtualTopic);
            physical.put(config.topic(), virtualTopic);
            names.put(v, config.topic());
        });
        this.snapshot = new Snapshot(
                Collections.unmodifiableMap(virtual),
                Collections.unmodifiableMap(physical),
                Collections.unmodifiableMap(names));
    }

    public Optional<VirtualTopic> findByName(String name) {
        return this.snapshot.byVirtual.values()
                .stream()
                .filter(virtualTopic -> Objects.equals(name, virtualTopic.name))
                .findFirst();
    }

    /// Maps a client-visible name to the physical topic name the broker must see.
    public String toPhysical(String virtual) {
        VirtualTopic virtualTopic = snapshot.byVirtual().get(virtual);
        return virtualTopic == null ? virtual : virtualTopic.physical();
    }

    /// Maps a physical topic name back to the client-visible (virtual) name.
    public String toVirtual(String physical) {
        VirtualTopic virtualTopic = snapshot.byPhysical().get(physical);
        return virtualTopic == null ? physical : virtualTopic.name();
    }

    public boolean hasVirtualTopic(String physical) {
        return snapshot.byPhysical().containsKey(physical);
    }

    public int size() {
        return snapshot.byVirtual().size();
    }

    /// Virtual-to-physical virtual-topic map (unmodifiable).
    public Map<String, String> virtualTopics() {
        return snapshot.virtualToPhysical();
    }

    /// The configured consume filter for a virtual topic, looked up by either its virtual or
    /// physical name, or [Optional#empty] if the topic isn't virtualized or has no
    /// filter configured.
    public Optional<VirtualTopicFilterConfig> filterFor(String virtualOrPhysical) {
        return entryFor(virtualOrPhysical).map(VirtualTopic::filter);
    }

    /// The configured value encoding for a virtual topic, looked up by either its virtual or
    /// physical name, or [Optional#empty] if the topic isn't virtualized or has no format
    /// configured (values are then treated as raw strings).
    public Optional<PayloadFormatConfig> valueFormatFor(String virtualOrPhysical) {
        return entryFor(virtualOrPhysical).map(VirtualTopic::valueFormat);
    }

    private Optional<VirtualTopic> entryFor(String virtualOrPhysical) {
        Snapshot snap = snapshot;
        VirtualTopic virtualTopic = snap.byVirtual().get(virtualOrPhysical);
        if (virtualTopic == null) {
            virtualTopic = snap.byPhysical().get(virtualOrPhysical);
        }
        return Optional.ofNullable(virtualTopic);
    }

    /// Whether this virtual topic's physical name should still be listed alongside its virtual
    /// name in Metadata responses. `false` (hidden - physical renamed to virtual in place)
    /// for non-virtualized topics and for virtual topics that don't opt in.
    public boolean exposesPhysicalTopic(String virtualOrPhysical) {
        Snapshot snap = snapshot;
        VirtualTopic virtualTopic = snap.byVirtual().get(virtualOrPhysical);
        if (virtualTopic == null) {
            virtualTopic = snap.byPhysical().get(virtualOrPhysical);
        }
        return virtualTopic != null && virtualTopic.exposePhysicalTopic();
    }

    public record VirtualTopic(
            String name,
            String physical,
            VirtualTopicFilterConfig filter,
            boolean exposePhysicalTopic,
            PayloadFormatConfig valueFormat
    ) {
    }

    /// Immutable snapshot of all three lookup maps. Published as a unit so a reload can never
    /// be observed half-applied.
    private record Snapshot(
            Map<String, VirtualTopic> byVirtual,
            Map<String, VirtualTopic> byPhysical,
            Map<String, String> virtualToPhysical) {

    }
}
