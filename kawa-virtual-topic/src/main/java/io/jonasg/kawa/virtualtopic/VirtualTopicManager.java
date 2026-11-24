package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.virtualtopic.filter.AvroSchemaResolver;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.jonasg.kawa.virtualtopic.filter.VirtualTopicRecordFilter;
import io.jonasg.kawa.virtualtopic.mapping.HeaderMappings;
import io.jonasg.kawa.virtualtopic.mapping.ValueMapping;
import io.jonasg.kawa.virtualtopic.mapping.ValueMappings;
import io.jonasg.kawa.virtualtopic.mapping.VirtualTopicRecordTransformer;
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
/// The whole mapping is an immutable [State] swapped atomically via a single `volatile` reference.
/// Changing it takes two steps: [#prepare] builds the complete new state without changing what the
/// manager answers, and [#publish] makes it current in one write, so a reader on the hot path
/// observes either the previous or the new mapping in its entirety - never a mix of the two. A
/// caller applying a configuration that spans several components can prepare first, so an invalid
/// one is refused before any part of it takes effect; [#reload] does both at once.
///
/// Each entry also carries what fetches need prepared - the topic's compiled consume filter and its
/// value transformer with their codecs - built by [#prepare] from the configuration alone. So they
/// exist only for the virtual topics of the current mapping: a reload that changes or removes a topic
/// leaves nothing of its previous configuration behind.
///
/// Topic names that are not virtualized map to themselves (identity) and carry no filter.
public final class VirtualTopicManager {

    private final SchemaRegistries schemaRegistries;
    private volatile State state;

    /// A manager whose topics' clusters have no schema registry.
    public VirtualTopicManager(Map<String, VirtualTopicConfig> virtualTopics) {
        this(virtualTopics, SchemaRegistries.NONE);
    }

    /// @param schemaRegistries finds the schema registry of a physical topic's cluster, to decode
    ///                         Avro record values for consume filters and transformations, and to
    ///                         encode transformed values as Avro
    public VirtualTopicManager(Map<String, VirtualTopicConfig> virtualTopics, SchemaRegistries schemaRegistries) {
        this.schemaRegistries = schemaRegistries;
        reload(virtualTopics);
    }

    /// Replaces the virtual-topic mapping with one built from `virtualTopics`: [#publish] of
    /// [#prepare]. An invalid transformation throws and leaves the previous mapping in place.
    public void reload(Map<String, VirtualTopicConfig> virtualTopics) {
        publish(prepare(virtualTopics));
    }

    /// Builds the complete state for `virtualTopics` - the mapping and each topic's compiled
    /// transformations, consume filter and value transformer, so no fetch prepares them - without
    /// changing what this manager answers. Pass the result to [#publish] to make it current.
    ///
    /// @throws IllegalArgumentException naming the virtual topic and step whose transformations are invalid
    public State prepare(Map<String, VirtualTopicConfig> virtualTopics) {
        Map<String, VirtualTopic> virtual = new LinkedHashMap<>();
        Map<String, List<VirtualTopic>> physical = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        virtualTopics.forEach((v, config) -> {
            VirtualTopic virtualTopic = virtualTopic(v, config);
            virtual.put(v, virtualTopic);
            physical.computeIfAbsent(config.topic(), _ -> new ArrayList<>()).add(virtualTopic);
            names.put(v, config.topic());
        });
        physical.replaceAll((_, topics) -> List.copyOf(topics));
        return new State(
                Collections.unmodifiableMap(virtual),
                Collections.unmodifiableMap(physical),
                Collections.unmodifiableMap(names));
    }

    /// Makes `state`, built by [#prepare], the current mapping.
    ///
    /// Safe to call concurrently with readers: the state is assigned to a single `volatile`
    /// reference, so readers see either the previous or the new mapping, never a partially-applied one.
    public void publish(State state) {
        this.state = Objects.requireNonNull(state, "state");
    }

    private VirtualTopic virtualTopic(String name, VirtualTopicConfig config) {
        ValueMapping mapping = mapping(name, config);
        PayloadFormatConfig outputFormat = config.outputFormat();
        AvroSchemaResolver avroSchemas = schemaRegistries.avroSchemasFor(config.topic());
        VirtualTopicRecordFilter recordFilter = config.filter() == null
                ? null
                : VirtualTopicRecordFilter.of(name, config.filter(), config.valueFormat(), avroSchemas);
        VirtualTopicRecordTransformer transformer = config.transformations().isEmpty()
                ? null
                : VirtualTopicRecordTransformer.of(config.valueFormat(), mapping,
                        HeaderMappings.compile(config.transformations()), outputFormat, avroSchemas,
                        schemaRegistries.avroSubjectsFor(config.topic()), config.transformations());
        return new VirtualTopic(name, config.topic(), config.filter(), config.exposePhysicalTopic(),
                config.valueFormat(), mapping, outputFormat, recordFilter, transformer);
    }

    private static ValueMapping mapping(String virtualTopic, VirtualTopicConfig config) {
        try {
            return ValueMappings.compile(config.transformations());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid transformations for virtual topic '" + virtualTopic + "': " + e.getMessage(), e);
        }
    }

    public Optional<VirtualTopic> findByName(String name) {
        return this.state.byVirtual().values()
                .stream()
                .filter(virtualTopic -> Objects.equals(name, virtualTopic.name))
                .findFirst();
    }

    /// Maps a client-visible name to the physical topic name the broker must see.
    public String toPhysical(String virtual) {
        VirtualTopic virtualTopic = state.byVirtual().get(virtual);
        return virtualTopic == null ? virtual : virtualTopic.physical();
    }

    /// Maps a physical topic name back to the client-visible (virtual) names: every virtual topic
    /// that points at it, or the physical name itself when none does.
    public List<String> toVirtual(String physical) {
        List<VirtualTopic> virtualTopics = state.byPhysical().get(physical);
        return virtualTopics == null ? List.of(physical) : virtualTopics.stream().map(VirtualTopic::name).toList();
    }

    /// The virtual topics among `names` that share their physical topic with another of them, keyed by that
    /// physical topic, in the order the names came. A request or consumer group may use at most one virtual topic
    /// per physical topic: the broker keeps records, fetch positions and committed offsets per physical partition,
    /// so two such names would collide. Empty when none collide; names that are not virtual topics never do.
    public Map<String, Set<String>> sharedPhysicalTopics(Collection<String> names) {
        State current = state;
        Map<String, Set<String>> byPhysical = new LinkedHashMap<>();
        for (String name : names) {
            VirtualTopic virtualTopic = current.byVirtual().get(name);
            if (virtualTopic != null) {
                byPhysical.computeIfAbsent(virtualTopic.physical(), _ -> new LinkedHashSet<>()).add(name);
            }
        }
        byPhysical.values().removeIf(virtual -> virtual.size() < 2);
        return byPhysical;
    }

    public boolean hasVirtualTopic(String physical) {
        return state.byPhysical().containsKey(physical);
    }

    public int size() {
        return state.byVirtual().size();
    }

    /// Virtual-to-physical virtual-topic map (unmodifiable).
    public Map<String, String> virtualTopics() {
        return state.virtualToPhysical();
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
        return Optional.ofNullable(state.byVirtual().get(virtual));
    }

    /// @param mapping      the compiled value steps, [ValueMapping#IDENTITY] when there are none
    /// @param outputFormat the format delivered values are in: the `convert` format, else `valueFormat`;
    ///                     `null` when there are no transformations, or only header steps without a `valueFormat`
    /// @param recordFilter `filter` prepared for fetched records, `null` when there is no filter
    /// @param transformer  the transformations prepared for fetched records, `null` when there are none
    public record VirtualTopic(
            String name,
            String physical,
            VirtualTopicFilterConfig filter,
            boolean exposePhysicalTopic,
            PayloadFormatConfig valueFormat,
            ValueMapping mapping,
            @Nullable PayloadFormatConfig outputFormat,
            @Nullable VirtualTopicRecordFilter recordFilter,
            @Nullable VirtualTopicRecordTransformer transformer
    ) {

        /// Whether fetched record values are transformed before they reach the client.
        public boolean transformsValues() {
            return transformer != null;
        }
    }

    /// Immutable state of all three lookup maps, built by [#prepare] and made current by [#publish]
    /// as a unit, so a reload can never be observed half-applied. Opaque to callers.
    public static final class State {

        private final Map<String, VirtualTopic> byVirtual;
        private final Map<String, List<VirtualTopic>> byPhysical;
        private final Map<String, String> virtualToPhysical;

        private State(
                Map<String, VirtualTopic> byVirtual,
                Map<String, List<VirtualTopic>> byPhysical,
                Map<String, String> virtualToPhysical) {
            this.byVirtual = byVirtual;
            this.byPhysical = byPhysical;
            this.virtualToPhysical = virtualToPhysical;
        }

        private Map<String, VirtualTopic> byVirtual() {
            return byVirtual;
        }

        private Map<String, List<VirtualTopic>> byPhysical() {
            return byPhysical;
        }

        private Map<String, String> virtualToPhysical() {
            return virtualToPhysical;
        }
    }
}
