package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.virtualtopic.mapping.ValueMapping;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    @Test
    void compilesTheTransformationsOfAVirtualTopic() {
        // given
        var manager = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(),
                List.of(new DropFieldsTransformationConfig(List.of("secret"))), null)));

        // then
        assertThat(manager.findByName("orders").orElseThrow().mapping())
                .withFailMessage(() -> "The virtual topic's transformations were not compiled")
                .isNotSameAs(ValueMapping.IDENTITY);
        assertThat(virtualTopics.findByName("orders").orElseThrow().mapping())
                .withFailMessage(() -> "A virtual topic without transformations did not get the identity mapping")
                .isSameAs(ValueMapping.IDENTITY);
    }

    @Test
    void keepsThePreviousMappingWhenATransformationDoesNotCompile() {
        // given
        var invalid = new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(),
                List.of(new CelTransformationConfig("value.")), null);

        // when / then
        assertThatThrownBy(() -> virtualTopics.reload(Map.of("orders", invalid)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid transformations for virtual topic 'orders'");
        assertThat(virtualTopics.toPhysical("orders"))
                .withFailMessage(() -> "A failed reload replaced the previous mapping")
                .isEqualTo("orders-v2");
    }

    @Test
    void keepsAnsweringThePreviousMappingUntilAPreparedStateIsPublished() {
        // when
        virtualTopics.prepare(Map.of("payments", new VirtualTopicConfig("payments-v1")));

        // then
        assertThat(virtualTopics.toPhysical("orders"))
                .withFailMessage(() -> "Preparing a mapping stopped 'orders' from mapping to 'orders-v2' before it was published")
                .isEqualTo("orders-v2");
        assertThat(virtualTopics.toPhysical("payments"))
                .withFailMessage(() -> "Preparing a mapping made 'payments' a virtual topic before it was published")
                .isEqualTo("payments");
        assertThat(virtualTopics.virtualTopics())
                .withFailMessage(() -> "Preparing a mapping changed the virtual topics before it was published, got %s"
                        .formatted(virtualTopics.virtualTopics()))
                .containsOnlyKeys("orders", "customers", "legacy");
    }

    @Test
    void answersExactlyTheMappingOfThePublishedState() {
        // given
        VirtualTopicManager.State state = virtualTopics.prepare(Map.of("payments", new VirtualTopicConfig("payments-v1")));

        // when
        virtualTopics.publish(state);

        // then
        assertThat(virtualTopics.toPhysical("payments"))
                .withFailMessage(() -> "The published mapping did not map 'payments' to 'payments-v1'")
                .isEqualTo("payments-v1");
        assertThat(virtualTopics.virtualTopics())
                .withFailMessage(() -> "The published mapping did not replace the previous virtual topics, got %s"
                        .formatted(virtualTopics.virtualTopics()))
                .containsOnlyKeys("payments");
    }

    @Test
    void refusesToPrepareAMappingWithAnInvalidTransformationNamingTheVirtualTopic() {
        // given
        var invalid = new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(),
                List.of(new CelTransformationConfig("value.")), null);

        // when / then
        assertThatThrownBy(() -> virtualTopics.prepare(Map.of("orders", invalid)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("virtual topic 'orders'");
        assertThat(virtualTopics.toPhysical("orders"))
                .withFailMessage(() -> "A refused mapping replaced the mapping of 'orders' to 'orders-v2'")
                .isEqualTo("orders-v2");
    }

    @Test
    void acceptsAMappingWhoseConsumeFilterDoesNotCompile() {
        // given
        var unfilterable = new VirtualTopicConfig("orders.raw", new CelFilterConfig("value.a >"), false, new JsonFormatConfig());

        // when / then
        assertThatCode(() -> virtualTopics.reload(Map.of("orders", unfilterable)))
                .withFailMessage(() -> "A consume filter that does not compile refused the whole mapping")
                .doesNotThrowAnyException();
    }
}
