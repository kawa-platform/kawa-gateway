package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.TopicDescriber;
import io.jonasg.kawa.governance.TopicSpec;

/// Broker topic operations for the admin HTTP surface. Implementations talk to the real
/// cluster; [KafkaTopicAdmin] uses Kafka's AdminClient. Describing ([TopicDescriber]) reads a
/// topic's current state for governance.
public interface TopicAdmin extends TopicDescriber, AutoCloseable {

    /// Creates the topic on the broker. Throws when the broker rejects the request
    /// (e.g. `TopicExistsException` when the topic already exists).
    void createTopic(TopicSpec spec) throws Exception;

    /// Deletes the topic on the broker.
    void deleteTopic(String name) throws Exception;

    @Override
    void close();
}
