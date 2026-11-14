package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.http.topics.TopicService.VirtualTopicEntry;
import io.jonasg.kawa.mcp.ConsumerGroupView.VirtualTopicReaderView;
import io.jonasg.kawa.mcp.TopicsView.FilterView;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// The virtual topics that read each physical topic, as `list_topics` reports them. Consumer group
/// offsets belong to the physical topic, so this is how a group's lag is tied back to the virtual
/// topics its consumers use.
final class VirtualTopicReaders {

    private final Map<String, List<VirtualTopicEntry>> byPhysicalTopic;

    private VirtualTopicReaders(Map<String, List<VirtualTopicEntry>> byPhysicalTopic) {
        this.byPhysicalTopic = byPhysicalTopic;
    }

    static VirtualTopicReaders of(TopicService topics) {
        return new VirtualTopicReaders(topics.listTopics().virtual().stream()
                .collect(Collectors.groupingBy(VirtualTopicEntry::physicalTopic)));
    }

    /// The virtual topics backed by `physicalTopic`, sorted by name.
    List<VirtualTopicReaderView> readersOf(String physicalTopic) {
        return byPhysicalTopic.getOrDefault(physicalTopic, List.of()).stream()
                .sorted(Comparator.comparing(VirtualTopicEntry::name))
                .map(entry -> new VirtualTopicReaderView(entry.name(), FilterView.of(entry.filter())))
                .toList();
    }

    /// Whether a virtual topic backed by `physicalTopic` filters records. Records it drops still
    /// count as lag, though its consumers never receive them, so the lag is then an upper bound.
    boolean filters(String physicalTopic) {
        return byPhysicalTopic.getOrDefault(physicalTopic, List.of()).stream().anyMatch(entry -> entry.filter() != null);
    }
}
