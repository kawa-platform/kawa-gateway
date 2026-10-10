package io.jonasg.kawa.core.module;

import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/// Which gateway modules are switched on, reloaded from every config-topic snapshot like the other
/// dynamic holders. One instance is shared by the whole gateway.
///
/// A module without an entry in the snapshot is enabled, so only an explicit `enabled: false`
/// switches one off.
///
/// Safe to read from any thread while [#reload] runs: the enabled set is immutable and published
/// through a single `volatile` field, so readers see either the previous or the new set, never a
/// partially-applied one. A caller that checks several modules and needs them consistent should
/// read [#enabled] once and query that set. Components that must react to a change, rather than
/// read the set when they need it, [#subscribe] instead.
public final class ModuleRegistry {

    private final List<Consumer<Set<ModuleId>>> subscribers = new CopyOnWriteArrayList<>();

    private volatile Set<ModuleId> enabled = Set.copyOf(EnumSet.allOf(ModuleId.class));

    public boolean isEnabled(ModuleId id) {
        return enabled.contains(id);
    }

    /// The modules switched on, as an immutable snapshot.
    public Set<ModuleId> enabled() {
        return enabled;
    }

    /// Calls `subscriber` with the current enabled set now, and again with the new set every time
    /// it changes. Calls happen on the thread that reloads the config.
    public synchronized void subscribe(Consumer<Set<ModuleId>> subscriber) {
        subscribers.add(subscriber);
        subscriber.accept(enabled);
    }

    /// Replaces the enabled set with the one `modules` describes, and notifies the subscribers when
    /// it changed.
    public synchronized void reload(Map<String, ModuleConfig> modules) {
        Set<ModuleId> next = Arrays.stream(ModuleId.values())
                .filter(id -> ModuleConfig.stateOf(id, modules).enabled())
                .collect(Collectors.toUnmodifiableSet());
        if (next.equals(enabled)) {
            return;
        }
        enabled = next;
        subscribers.forEach(subscriber -> subscriber.accept(next));
    }
}
