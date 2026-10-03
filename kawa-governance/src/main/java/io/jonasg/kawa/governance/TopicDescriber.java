package io.jonasg.kawa.governance;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/// Reads the current state of physical topics from the broker, so a change can be judged
/// against what the topic will look like after it. Must not block.
public interface TopicDescriber {

    /// The current state of each topic that exists; a topic the broker does not know is left
    /// out of the map. The stage fails when the broker cannot be asked at all.
    CompletionStage<Map<String, TopicState>> describe(Collection<String> topics);

    /// A topic as the broker has it now.
    ///
    /// @param partitions        the partition count
    /// @param replicationFactor the replication factor
    /// @param configs           the configs set on the topic itself (overrides); broker and
    ///                          cluster defaults are left out, matching what a create sends
    record TopicState(int partitions, int replicationFactor, Map<String, String> configs) {

        public TopicState {
            configs = configs == null ? Map.of() : Map.copyOf(configs);
        }
    }
}
