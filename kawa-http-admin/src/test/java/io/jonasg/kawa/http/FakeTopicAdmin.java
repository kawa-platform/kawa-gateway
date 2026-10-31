package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.http.topics.TopicAdmin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// In-memory [TopicAdmin] for handler tests: records create/delete calls and describes from
/// [#states] instead of talking to a broker.
public final class FakeTopicAdmin implements TopicAdmin {

    public final List<TopicSpec> created = new ArrayList<>();
    public final List<String> deleted = new ArrayList<>();
    public RuntimeException createError;
    /// What [#describe] reports, by topic name.
    public final Map<String, TopicState> states = new HashMap<>();

    @Override
    public void createTopic(TopicSpec spec) {
        if (createError != null) {
            throw createError;
        }
        created.add(spec);
    }

    @Override
    public void deleteTopic(String name) {
        deleted.add(name);
    }

    @Override
    public CompletionStage<Map<String, TopicState>> describe(Collection<String> topics) {
        Map<String, TopicState> found = new HashMap<>();
        topics.forEach(name -> {
            if (states.containsKey(name)) {
                found.put(name, states.get(name));
            }
        });
        return CompletableFuture.completedFuture(found);
    }

    @Override
    public void close() {
    }
}
