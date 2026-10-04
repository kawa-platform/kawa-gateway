package io.jonasg.kawa.http.auth;

import io.jonasg.kawa.plugin.auth.ProviderSettings;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/// [ProviderSettings] over the `admin.auth.settings` map (environment placeholders already resolved
/// by kawa-config).
public final class MapProviderSettings implements ProviderSettings {

    private final Map<String, Object> settings;

    public MapProviderSettings(Map<String, Object> settings) {
        this.settings = Map.copyOf(settings);
    }

    @Override
    public Optional<String> string(String key) {
        Object value = settings.get(key);
        return value == null || value.toString().isBlank() ? Optional.empty() : Optional.of(value.toString());
    }

    @Override
    public String requireString(String key) {
        return string(key).orElseThrow(() -> new IllegalArgumentException(name(key) + " is required"));
    }

    @Override
    public Optional<Duration> duration(String key) {
        return string(key).map(value -> {
            try {
                return Duration.parse(value);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(name(key) + " is not an ISO-8601 duration (e.g. PT15M): " + value);
            }
        });
    }

    @Override
    public List<String> strings(String key) {
        Object value = settings.get(key);
        Stream<String> values = switch (value) {
            case null -> Stream.empty();
            case List<?> list -> list.stream().map(String::valueOf);
            default -> Arrays.stream(value.toString().split(","));
        };
        return values.map(String::trim).filter(item -> !item.isEmpty()).toList();
    }

    private static String name(String key) {
        return "admin.auth.settings." + key;
    }
}
