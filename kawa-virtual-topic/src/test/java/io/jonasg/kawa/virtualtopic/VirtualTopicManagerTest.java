package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualTopicManagerTest {

    private final VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of(
            "orders", new VirtualTopicConfig("orders-v2"),
            "customers", new VirtualTopicConfig("crm.customers",
                    new HeaderEqualsFilterConfig("tenant", "acme")),
            "legacy", new VirtualTopicConfig("legacy-v1", null, true)));

    @Test
    void mapsVirtualToPhysical() {
        assertThat(virtualTopics.toPhysical("orders")).isEqualTo("orders-v2");
        assertThat(virtualTopics.toPhysical("customers")).isEqualTo("crm.customers");
    }

    @Test
    void mapsPhysicalToVirtual() {
        assertThat(virtualTopics.toVirtual("orders-v2")).containsExactly("orders");
        assertThat(virtualTopics.toVirtual("crm.customers")).containsExactly("customers");
    }

    @Test
    void identityForNonVirtualTopics() {
        assertThat(virtualTopics.toPhysical("plain")).isEqualTo("plain");
        assertThat(virtualTopics.toVirtual("plain")).containsExactly("plain");
    }

    @Test
    void exposesImmutableVirtualTopics() {
        assertThatThrownBy(() -> virtualTopics.virtualTopics().put("a", "b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void filterForReturnsConfiguredFilterOnlyByVirtualName() {
        var expected = new HeaderEqualsFilterConfig("tenant", "acme");

        assertThat(virtualTopics.filterFor("customers"))
                .withFailMessage(() -> "Filter configured on virtual topic 'customers' was not returned for that name")
                .contains(expected);
        assertThat(virtualTopics.filterFor("crm.customers"))
                .withFailMessage(() -> "Physical topic 'crm.customers' unexpectedly carried a filter")
                .isEmpty();
    }

    @Test
    void filterForIsEmptyWhenNoFilterConfigured() {
        assertThat(virtualTopics.filterFor("orders")).isEmpty();
        assertThat(virtualTopics.filterFor("orders-v2")).isEmpty();
    }

    @Test
    void filterForIsEmptyForNonVirtualTopics() {
        assertThat(virtualTopics.filterFor("plain")).isEmpty();
    }

    @Test
    void exposesPhysicalTopicIsFalseByDefault() {
        assertThat(virtualTopics.exposesPhysicalTopic("orders")).isFalse();
        assertThat(virtualTopics.exposesPhysicalTopic("orders-v2")).isFalse();
    }

    @Test
    void exposesPhysicalTopicWhenOptedIn() {
        assertThat(virtualTopics.exposesPhysicalTopic("legacy"))
                .withFailMessage(() -> "Virtual topic 'legacy' opted in to exposing its physical topic but was not exposed")
                .isTrue();
        assertThat(virtualTopics.exposesPhysicalTopic("legacy-v1"))
                .withFailMessage(() -> "Physical topic 'legacy-v1' was exposed although only the virtual name 'legacy' opted in")
                .isFalse();
    }

    @Test
    void exposesPhysicalTopicIsFalseForNonVirtualTopics() {
        assertThat(virtualTopics.exposesPhysicalTopic("plain")).isFalse();
    }

    @Test
    void reloadReplacesSnapshot() {
        // given
        var manager = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));

        // when
        manager.reload(Map.of("customers", new VirtualTopicConfig("crm.customers")));

        // then
        assertThat(manager.toPhysical("orders")).isEqualTo("orders");
        assertThat(manager.toPhysical("customers")).isEqualTo("crm.customers");
        assertThat(manager.size()).isEqualTo(1);
    }

    @Test
    void reloadWithEmptyMapClearsAllMappings() {
        // given
        var manager = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));

        // when
        manager.reload(Map.of());

        // then
        assertThat(manager.toPhysical("orders")).isEqualTo("orders");
        assertThat(manager.size()).isZero();
        assertThat(manager.virtualTopics()).isEmpty();
    }

    @Test
    void reloadCarriesFilterAndExposePhysicalTopic() {
        // given
        var manager = new VirtualTopicManager(Map.of());

        // when
        manager.reload(Map.of(
                "customers", new VirtualTopicConfig("crm.customers",
                        new HeaderEqualsFilterConfig("tenant", "acme")),
                "legacy", new VirtualTopicConfig("legacy-v1", null, true)));

        // then
        assertThat(manager.filterFor("customers"))
                .contains(new HeaderEqualsFilterConfig("tenant", "acme"));
        assertThat(manager.exposesPhysicalTopic("legacy")).isTrue();
    }

    @Test
    void reloadIsSafeDuringConcurrentReads() throws Exception {
        // given
        var manager = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));
        var first = Map.of("orders", new VirtualTopicConfig("orders-v2"));
        var second = Map.of("customers", new VirtualTopicConfig("crm.customers",
                new HeaderEqualsFilterConfig("tenant", "acme")));
        var failure = new AtomicReference<Throwable>();

        // when
        var writer = new Thread(() -> {
            for (int i = 0; i < 10_000; i++) {
                manager.reload(i % 2 == 0 ? first : second);
            }
        });
        var reader = new Thread(() -> {
            try {
                for (int i = 0; i < 10_000; i++) {
                    manager.toPhysical("orders");
                    manager.toPhysical("customers");
                    manager.toVirtual("orders-v2");
                    manager.toVirtual("crm.customers");
                    manager.filterFor("customers");
                    manager.exposesPhysicalTopic("legacy-v1");
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        writer.start();
        reader.start();
        writer.join();
        reader.join();

        // then
        assertThat(failure.get()).isNull();
    }

    @Test
    void mapsPhysicalTopicToEveryVirtualTopicOnIt() {
        // given
        var manager = new VirtualTopicManager(Map.of(
                "orders-eu", new VirtualTopicConfig("orders-v2"),
                "orders-us", new VirtualTopicConfig("orders-v2")));

        // when
        List<String> virtual = manager.toVirtual("orders-v2");

        // then
        assertThat(virtual)
                .withFailMessage(() -> "Physical topic 'orders-v2' did not map to both 'orders-eu' and 'orders-us' but to %s"
                        .formatted(virtual))
                .containsExactlyInAnyOrder("orders-eu", "orders-us");
    }
}
