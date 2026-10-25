package io.jonasg.kawa.config;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/// Loads the [BootstrapConfig] from the startup YAML file.
///
/// The file holds only bootstrap settings. Unknown keys fail the load, and so do dynamic sections
/// such as `rbac`, `virtualTopics`, `governance` or `auth.clients`: those are [DynamicConfig],
/// managed through the admin API and the config topic, and would otherwise be silently ignored.
public final class ConfigLoader {

    private final YAMLMapper mapper = YAMLMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public BootstrapConfig load(Path path) {
        String yaml;
        try {
            yaml = Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load gateway configuration from " + path, e);
        }
        return parse(yaml, path.toString());
    }

    public BootstrapConfig loadFromYaml(String yaml) {
        return parse(yaml, "YAML");
    }

    private BootstrapConfig parse(String yaml, String source) {
        try {
            return mapper.readValue(yaml, BootstrapConfig.class);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Invalid gateway configuration in " + source + ": " + e.getMessage(), e);
        }
    }
}
