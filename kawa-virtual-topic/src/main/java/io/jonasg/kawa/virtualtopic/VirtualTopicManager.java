package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.virtualtopic.mapping.ValueMapping;
import io.jonasg.kawa.virtualtopic.mapping.ValueMappings;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
    ///
    /// Each virtual topic's transformations are compiled first; an invalid one throws (see
    /// [#validate]) and leaves the previous snapshot in place.
    public void reload(Map<String, VirtualTopicConfig> virtualTopics) {
        Map<String, VirtualTopic> virtual = new LinkedHashMap<>();
        Map<String, List<VirtualTopic>> physical = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        virtualTopics.forEach((v, config) -> {
            VirtualTopic virtualTopic = new VirtualTopic(v, config.topic(), config.filter(), config.exposePhysicalTopic(),
                    config.valueFormat(), mapping(v, config), config.outputFormat());
            virtual.put(v, virtualTopic);
            physical.computeIfAbsent(config.topic(), _ -> new ArrayList<>()).add(virtualTopic);
            names.put(v, config.topic());
        });
        physical.replaceAll((_, topics) -> List.copyOf(topics));
        this.snapshot = new Snapshot(
                Collections.unmodifiableMap(virtual),
                Collections.unmodifiableMap(physical),
                Collections.unmodifiableMap(names));
    }

    /// Compiles the transformations of every virtual topic without applying anything, so a
    /// configuration can be refused before any part of it takes effect.
    ///
    /// @throws IllegalArgumentException naming the virtual topic and step that is invalid
    public static void validate(Map<String, VirtualTopicConfig> virtualTopics) {
        virtualTopics.forEach(VirtualTopicManager::mapping);
    }

    private static ValueMapping mapping(String virtualTopic, VirtualTopicConfig config) {
        try {
            return ValueMappings.compile(config.transformations());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid transformations for virtual topic '" + virtualTopic + "': " + e.getMessage(), e);
        }
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

    /// Maps a physical topic name back to the client-visible (virtual) names: every virtual topic
    /// that points at it, or the physical name itself when none does.
    public List<String> toVirtual(String physical) {
        List<VirtualTopic> virtualTopics = snapshot.byPhysical().get(physical);
        return virtualTopics == null ? List.of(physical) : virtualTopics.stream().map(VirtualTopic::name).toList();
    }

    /// The virtual topics among `names` that share their physical topic with another of them, keyed by that
    /// physical topic, in the order the names came. A request or consumer group may use at most one virtual topic
    /// per physical topic: the broker keeps records, fetch positions and committed offsets per physical partition,
    /// so two such names would collide. Empty when none collide; names that are not virtual topics never do.
    public Map<String, Set<String>> sharedPhysicalTopics(Collection<String> names) {
        Snapshot snap = snapshot;
        Map<String, Set<String>> byPhysical = new LinkedHashMap<>();
        for (String name : names) {
            VirtualTopic virtualTopic = snap.byVirtual().get(name);
            if (virtualTopic != null) {
                byPhysical.computeIfAbsent(virtualTopic.physical(), _ -> new LinkedHashSet<>()).add(name);
            }
        }
        byPhysical.values().removeIf(virtual -> virtual.size() < 2);
        return byPhysical;
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

    /// The configured consume filter for the virtual topic `virtual`, or [Optional#empty] if it has no filter
    /// configured. A physical or unknown name has none: a filter belongs to the virtual topic, never to the
    /// physical topic it reads.
    public Optional<VirtualTopicFilterConfig> filterFor(String virtual) {
        return virtualTopic(virtual).map(VirtualTopic::filter);
    }

    /// The configured value encoding for the virtual topic `virtual`, or [Optional#empty] if it has no format
    /// configured (values are then treated as raw strings). A physical or unknown name has none.
    public Optional<PayloadFormatConfig> valueFormatFor(String virtual) {
        return virtualTopic(virtual).map(VirtualTopic::valueFormat);
    }

    /// Whether the virtual topic `virtual` also lists its physical topic under the physical name in Metadata
    /// responses. `false` (physical renamed to virtual in place) for virtual topics that don't opt in and for
    /// names that are not virtual topics.
    public boolean exposesPhysicalTopic(String virtual) {
        return virtualTopic(virtual).map(VirtualTopic::exposePhysicalTopic).orElse(false);
    }

    /// The virtual topic named `virtual`, or [Optional#empty] if no virtual topic has that name.
    /// A map lookup, unlike [#findByName], so it suits the hot path.
    public Optional<VirtualTopic> virtualTopic(String virtual) {
        return Optional.ofNullable(snapshot.byVirtual().get(virtual));
    }

    /// @param mapping      the compiled transformations, [ValueMapping#IDENTITY] when there are none
    /// @param outputFormat the format transformed values are encoded in, `null` when there are no
    ///                     transformations (values are then passed through unchanged)
    public record VirtualTopic(
            String name,
            String physical,
            VirtualTopicFilterConfig filter,
            boolean exposePhysicalTopic,
            PayloadFormatConfig valueFormat,
            ValueMapping mapping,
            @Nullable PayloadFormatConfig outputFormat
    ) {

        /// Whether fetched record values are transformed before they reach the client.
        public boolean transformsValues() {
            return outputFormat != null;
        }
    }

    /// Immutable snapshot of all three lookup maps. Published as a unit so a reload can never
    /// be observed half-applied.
    private record Snapshot(
            Map<String, VirtualTopic> byVirtual,
            Map<String, List<VirtualTopic>> byPhysical,
            Map<String, String> virtualToPhysical) {

    }
}
