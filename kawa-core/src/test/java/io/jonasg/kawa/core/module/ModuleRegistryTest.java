package io.jonasg.kawa.core.module;

import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ModuleRegistryTest {

    private final ModuleRegistry registry = new ModuleRegistry();

    @Test
    void enablesEveryModuleByDefault() {
        // then
        assertThat(registry.enabled())
                .withFailMessage(() -> "Not every module is enabled by default: " + registry.enabled())
                .containsExactlyInAnyOrder(ModuleId.values());
    }

    @Test
    void enablesEveryModuleWhenTheConfigHasNoModules() {
        // when
        registry.reload(Map.of());

        // then
        assertThat(registry.enabled())
                .withFailMessage(() -> "Not every module is enabled for an empty modules section: " + registry.enabled())
                .containsExactlyInAnyOrder(ModuleId.values());
    }

    @Test
    void disablesOnlyTheModuleSwitchedOff() {
        // when
        registry.reload(Map.of(
                "rbac", new ModuleConfig(ModuleId.RBAC, false),
                "governance", new ModuleConfig(ModuleId.GOVERNANCE, true)));

        // then
        assertThat(registry.isEnabled(ModuleId.RBAC))
                .withFailMessage(() -> "Module 'rbac' is still enabled after it was switched off")
                .isFalse();
        assertThat(registry.enabled())
                .withFailMessage(() -> "Expected only 'rbac' to be off: " + registry.enabled())
                .containsExactlyInAnyOrder(ModuleId.VIRTUAL_TOPICS, ModuleId.GOVERNANCE);
    }

    @Test
    void reenablesAModuleOnTheNextReload() {
        // given
        registry.reload(Map.of("virtual-topics", new ModuleConfig(ModuleId.VIRTUAL_TOPICS, false)));

        // when
        registry.reload(Map.of("virtual-topics", new ModuleConfig(ModuleId.VIRTUAL_TOPICS, true)));

        // then
        assertThat(registry.isEnabled(ModuleId.VIRTUAL_TOPICS))
                .withFailMessage(() -> "Module 'virtual-topics' stayed off after it was switched back on")
                .isTrue();
    }

    @Test
    void tellsASubscriberTheCurrentSetRightAway() {
        // given
        registry.reload(Map.of("rbac", new ModuleConfig(ModuleId.RBAC, false)));
        List<Set<ModuleId>> seen = new ArrayList<>();

        // when
        registry.subscribe(seen::add);

        // then
        assertThat(seen)
                .withFailMessage(() -> "The subscriber was not told the current set on subscribing: " + seen)
                .containsExactly(Set.of(ModuleId.VIRTUAL_TOPICS, ModuleId.GOVERNANCE));
    }

    @Test
    void tellsASubscriberOnlyWhenTheSetChanges() {
        // given
        List<Set<ModuleId>> seen = new ArrayList<>();
        registry.subscribe(seen::add);
        var virtualTopicsOff = Map.of("virtual-topics", new ModuleConfig(ModuleId.VIRTUAL_TOPICS, false));

        // when
        registry.reload(virtualTopicsOff);
        registry.reload(virtualTopicsOff);
        registry.reload(Map.of());

        // then
        assertThat(seen)
                .withFailMessage(() -> "Expected the initial set, the switch-off and the switch-on, got: " + seen)
                .containsExactly(
                        Set.of(ModuleId.values()),
                        Set.of(ModuleId.RBAC, ModuleId.GOVERNANCE),
                        Set.of(ModuleId.values()));
    }
}
