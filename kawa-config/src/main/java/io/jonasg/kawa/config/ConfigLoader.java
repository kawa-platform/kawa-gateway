package io.jonasg.kawa.config;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/// Loads [GatewayConfig] from YAML using Jackson.
///
/// Governance is managed exclusively through the admin API and the config topic, never through
/// the file: a `governance` section in the YAML is dropped before binding, so it is neither
/// applied nor validated.
public final class ConfigLoader {

    private static final String GOVERNANCE = "governance";

    private final YAMLMapper mapper;

    public ConfigLoader() {
        this.mapper = YAMLMapper.builder()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    public GatewayConfig load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return bind(mapper.readTree(in));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load gateway configuration from " + path, e);
        }
    }

    public GatewayConfig loadFromYaml(String yaml) {
        return bind(mapper.readTree(yaml));
    }

    private GatewayConfig bind(JsonNode tree) {
        if (tree instanceof ObjectNode root) {
            root.remove(GOVERNANCE);
        }
        return mapper.treeToValue(tree, GatewayConfig.class);
    }
}
