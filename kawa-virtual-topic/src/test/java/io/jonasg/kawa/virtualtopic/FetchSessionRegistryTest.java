package io.jonasg.kawa.virtualtopic;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FetchSessionRegistryTest {

    private static final TopicPartition CUSTOMERS_0 = new TopicPartition("customers-v2", 0);
    private static final TopicPartition CUSTOMERS_1 = new TopicPartition("customers-v2", 1);

    private final Object client = new Object();
    private final FetchSessionRegistry registry = new FetchSessionRegistry();

    @Test
    void bindsSessionAndResolvesVirtualNames() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));

        assertThat(registry.hasSession(client, 1)).isTrue();
        assertThat(registry.virtualFor(client, 1, "customers-v2")).isEqualTo("customers");
        assertThat(registry.virtualFor(client, 1, "orders-v2")).isNull();
    }

    @Test
    void ignoresBindForZeroSessionIdOrEmptyMapping() {
        registry.bindSession(client, 0, Map.of("customers-v2", "customers"));
        registry.bindSession(client, 2, Map.of());

        assertThat(registry.hasSession(client, 0)).isFalse();
        assertThat(registry.hasSession(client, 2)).isFalse();
    }

    @Test
    void mergesAndForgetsOnFetchRequest() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));

        registry.onFetchRequest(client, 1, Map.of("orders-v2", "orders"), List.of());
        assertThat(registry.virtualFor(client, 1, "orders-v2")).isEqualTo("orders");

        registry.onFetchRequest(client, 1, Map.of(), List.of("customers-v2"));
        assertThat(registry.virtualFor(client, 1, "customers-v2")).isNull();
    }

    @Test
    void removesSessionAndDropsAllOnDisconnect() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));
        registry.removeSession(client, 1);
        assertThat(registry.hasSession(client, 1)).isFalse();

        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));
        registry.bindSession(client, 2, Map.of("orders-v2", "orders"));
        registry.sessionClosed(client);
        assertThat(registry.hasSession(client, 1)).isFalse();
        assertThat(registry.hasSession(client, 2)).isFalse();
    }

    @Test
    void remembersTheFetchOffsetsOfTheFetchThatCreatedTheSession() {
        // given
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"), Map.of(CUSTOMERS_0, 5L, CUSTOMERS_1, 9L));

        // when / then
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_0))
                .withFailMessage(() -> "The fetch offset the client sent when creating the session was not remembered")
                .hasValue(5L);
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_1))
                .withFailMessage(() -> "The fetch offset of the client's other partition was not remembered")
                .hasValue(9L);
    }

    @Test
    void updatesTheFetchOffsetOfAPartitionAnIncrementalFetchNames() {
        // given
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"), Map.of(CUSTOMERS_0, 5L, CUSTOMERS_1, 9L));

        // when the client moved partition 0 on and left partition 1 out of its incremental fetch
        registry.onFetchRequest(client, 1, Map.of(), List.of(), Map.of(CUSTOMERS_0, 6L), List.of());

        // then
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_0))
                .withFailMessage(() -> "The client's new fetch offset was not remembered")
                .hasValue(6L);
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_1))
                .withFailMessage(() -> "The fetch offset of the partition the client left out was not kept")
                .hasValue(9L);
    }

    @Test
    void forgetsTheFetchOffsetOfAPartitionTheClientForgets() {
        // given
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"), Map.of(CUSTOMERS_0, 5L, CUSTOMERS_1, 9L));

        // when
        registry.onFetchRequest(client, 1, Map.of(), List.of(), Map.of(), List.of(CUSTOMERS_0));

        // then
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_0))
                .withFailMessage(() -> "The fetch offset of a partition the client forgot was kept")
                .isEmpty();
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_1))
                .withFailMessage(() -> "The fetch offset of a partition the client still fetches was dropped")
                .hasValue(9L);
    }

    @Test
    void forgetsTheFetchOffsetsWithTheSession() {
        // given
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"), Map.of(CUSTOMERS_0, 5L));

        // when
        registry.removeSession(client, 1);

        // then
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_0))
                .withFailMessage(() -> "The fetch offsets of a closed session were kept")
                .isEmpty();
    }

    @Test
    void startsTrackingASessionFirstSeenWithoutVirtualTopicsOnceAnIncrementalFetchNamesOne() {
        // given a session whose full fetch named only plain topics
        registry.bindSession(client, 1, Map.of(), Map.of(new TopicPartition("payments", 0), 3L));

        // when the client adds the virtual topic `customers` to the session through an incremental fetch
        registry.onFetchRequest(client, 1, Map.of("customers-v2", "customers"), List.of(), Map.of(CUSTOMERS_0, 5L), List.of());

        // then
        assertThat(registry.hasSession(client, 1))
                .withFailMessage(() -> "The session the client added a virtual topic to was not remembered")
                .isTrue();
        assertThat(registry.virtualFor(client, 1, "customers-v2"))
                .withFailMessage(() -> "The virtual topic the client added to its session was not remembered under the name it used")
                .isEqualTo("customers");
        assertThat(registry.fetchOffset(client, 1, CUSTOMERS_0))
                .withFailMessage(() -> "The fetch offset the client sent when adding the virtual topic to its session was not remembered")
                .hasValue(5L);
    }

    @Test
    void doesNotTrackASessionWhoseIncrementalFetchNamesNoVirtualTopic() {
        // given a session whose full fetch named only plain topics
        registry.bindSession(client, 1, Map.of(), Map.of(new TopicPartition("payments", 0), 3L));

        // when the client's incremental fetch names only plain topics again
        registry.onFetchRequest(client, 1, Map.of(), List.of(), Map.of(new TopicPartition("payments", 0), 4L), List.of());

        // then
        assertThat(registry.hasSession(client, 1))
                .withFailMessage(() -> "A session that never named a virtual topic was tracked")
                .isFalse();
    }

    @Test
    void sessionsAreScopedPerClient() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));

        assertThat(registry.hasSession(new Object(), 1)).isFalse();
    }
}
