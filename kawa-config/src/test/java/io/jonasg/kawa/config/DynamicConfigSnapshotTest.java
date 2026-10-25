package io.jonasg.kawa.config;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads [DynamicConfig] snapshots the way [ConfigTopicConsumer] reads them from the config topic.
class DynamicConfigSnapshotTest {

    private static final String PLAIN_S3CRET = "pbkdf2-sha256$600000$000102030405060708090a0b0c0d0e0f$"
            + "9bb2521bd15ed9f43200646a7fc90af2f03f560b074ce7a3e1d1d85914c0494c";
    private static final String SCRAM_HUNTER2 = "scram-sha256$4096$000102030405060708090a0b0c0d0e0f$"
            + "8a399d90117a55a52e469133c5ef0ff0dc992b3b871a4a25b890556790c5e7e4$"
            + "9da30c0f6abfc8a7c371381f361a59aac6e4ce8450c0e3df4ff4e01c43d63fbc";

    private final JsonMapper mapper = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Test
    void readsVirtualTopicsAsStringOrObject() {
        // when
        DynamicConfig config = read("""
                {"virtualTopics": {"orders": "orders-v2", "customers": {"topic": "crm.customers"}}}
                """);

        // then
        assertThat(config.virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"))
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
    }

    @Test
    void virtualTopicsAreImmutable() {
        // given
        DynamicConfig config = read("""
                {"virtualTopics": {"orders": "orders-v2"}}
                """);

        // when / then
        assertThatThrownBy(() -> config.virtualTopics().put("a", new VirtualTopicConfig("b")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidVirtualTopicMapping() {
        assertThatThrownBy(() -> read("""
                {"virtualTopics": {"orders": ["not-a-topic-map"]}}
                """))
                .hasMessageContaining("Invalid virtual topic mapping for topic 'orders'");
    }

    @Test
    void ignoresUnknownVirtualTopicKeys() {
        // when
        DynamicConfig config = read("""
                {"virtualTopics": {"customers": {"topic": "crm.customers", "unknown": "ignored"}}}
                """);

        // then
        assertThat(config.virtualTopics())
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
    }

    @Test
    void readsHeaderEqualsFilter() {
        assertThat(filterOf("""
                {"type": "headerEquals", "header": "tenant", "value": "acme"}
                """)).isEqualTo(new HeaderEqualsFilterConfig("tenant", "acme"));
    }

    @Test
    void readsCelFilter() {
        assertThat(filterOf("""
                {"type": "cel", "expression": "headers.tenant == \\"acme\\""}
                """)).isEqualTo(new CelFilterConfig("headers.tenant == \"acme\""));
    }

    @Test
    void readsHeaderContainsFilter() {
        assertThat(filterOf("""
                {"type": "headerContains", "header": "tenant", "value": "acm"}
                """)).isEqualTo(new HeaderContainsFilterConfig("tenant", "acm"));
    }

    @Test
    void readsHeaderStartsWithFilter() {
        assertThat(filterOf("""
                {"type": "headerStartsWith", "header": "tenant", "value": "ac"}
                """)).isEqualTo(new HeaderStartsWithFilterConfig("tenant", "ac"));
    }

    @Test
    void readsHeaderMatchesFilter() {
        assertThat(filterOf("""
                {"type": "headerMatches", "header": "tenant", "value": "eu.*"}
                """)).isEqualTo(new HeaderMatchesFilterConfig("tenant", "eu.*"));
    }

    @Test
    void rejectsInvalidHeaderMatchesRegex() {
        assertThatThrownBy(() -> filterOf("""
                {"type": "headerMatches", "header": "tenant", "value": "a{2,1}"}
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'");
    }

    @Test
    void rejectsVirtualTopicFilterWithoutType() {
        assertThatThrownBy(() -> filterOf("""
                {"header": "tenant", "value": "acme"}
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'")
                .hasMessageContaining("type");
    }

    @Test
    void rejectsUnknownVirtualTopicFilterType() {
        assertThatThrownBy(() -> filterOf("""
                {"type": "notARealType", "header": "tenant", "value": "acme"}
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'")
                .hasMessageContaining("type");
    }

    @Test
    void hidesThePhysicalTopicNameByDefault() {
        // when
        DynamicConfig config = read("""
                {"virtualTopics": {"orders": "orders-v2", "customers": {"topic": "crm.customers"}}}
                """);

        // then
        assertThat(config.virtualTopics().get("orders").exposePhysicalTopic()).isFalse();
        assertThat(config.virtualTopics().get("customers").exposePhysicalTopic()).isFalse();
    }

    @Test
    void readsExposePhysicalTopicOptIn() {
        // when
        DynamicConfig config = read("""
                {"virtualTopics": {"legacy": {"topic": "legacy-v1", "exposePhysicalTopic": true}}}
                """);

        // then
        assertThat(config.virtualTopics().get("legacy").exposePhysicalTopic()).isTrue();
    }

    @Test
    void rejectsNonBooleanExposePhysicalTopic() {
        assertThatThrownBy(() -> read("""
                {"virtualTopics": {"customers": {"topic": "crm.customers", "exposePhysicalTopic": "yes"}}}
                """))
                .hasMessageContaining("exposePhysicalTopic");
    }

    @Test
    void readsClientAuth() {
        // when
        DynamicConfig config = read("""
                {"auth": {
                  "mechanisms": ["PLAIN", "SCRAM-SHA-256"],
                  "clients": {
                    "alice": {"password": "%s"},
                    "bob": {"mechanism": "SCRAM-SHA-256", "password": "%s"}
                  }
                }}
                """.formatted(PLAIN_S3CRET, SCRAM_HUNTER2));

        // then
        assertThat(config.auth().mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
        assertThat(config.auth().clients()).hasSize(2);
        assertThat(config.auth().clients().get("alice").mechanism()).isEqualTo("PLAIN");
        assertThat(config.auth().clients().get("alice").password().verify("s3cret"))
                .withFailMessage(() -> "Loaded PLAIN password did not verify against the original plaintext")
                .isTrue();
        assertThat(config.auth().clients().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
        assertThat(config.auth().clients().get("bob").password().encoded()).isEqualTo(SCRAM_HUNTER2);
    }

    @Test
    void rejectsClientWithoutMechanismWhenNoGlobalMechanism() {
        assertThatThrownBy(() -> read("""
                {"auth": {"clients": {"alice": {"password": "%s"}}}}
                """.formatted(PLAIN_S3CRET)))
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void clientAuthIsImmutable() {
        // given
        DynamicConfig config = read("""
                {"auth": {"mechanisms": ["PLAIN"], "clients": {"alice": {"password": "%s"}}}}
                """.formatted(PLAIN_S3CRET));

        // when / then
        assertThatThrownBy(() -> config.auth().mechanisms().add("SCRAM-SHA-512"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.auth().clients().put("bob",
                new ClientConfig("PLAIN", HashedPassword.fromPlaintext(Mechanism.PLAIN, "pw"))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void readsRbac() {
        // when
        DynamicConfig config = read("""
                {"rbac": {
                  "roles": {
                    "producer": {"acls": [{"resource": {"type": "TOPIC", "pattern": "orders", "patternType": "LITERAL"},
                                           "operation": "WRITE", "permission": "ALLOW"}]},
                    "admin": {"acls": [{"resource": {"type": "CLUSTER"}, "operation": "CREATE"}]},
                    "consumer": {"acls": [{"resource": {"type": "GROUP", "pattern": "orders-group"}, "operation": "READ"}]}
                  },
                  "groups": {
                    "producers": {"clients": ["alice"], "roles": ["producer"]},
                    "admins": {"clients": ["bob"], "roles": ["admin"]},
                    "consumers": {"clients": ["carol"], "roles": ["consumer"]}
                  }
                }}
                """);

        // then
        RbacConfig rbac = config.rbac();
        assertThat(rbac.roles().keySet()).containsExactlyInAnyOrder("producer", "admin", "consumer");
        assertThat(rbac.roles().get("producer").acls()).containsExactly(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                        AclOperation.WRITE, AclPermissionType.ALLOW));
        assertThat(rbac.roles().get("admin").acls()).containsExactly(
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null, PatternType.LITERAL),
                        AclOperation.CREATE, AclPermissionType.ALLOW));
        assertThat(rbac.roles().get("consumer").acls()).containsExactly(
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "orders-group", PatternType.LITERAL),
                        AclOperation.READ, AclPermissionType.ALLOW));
        assertThat(rbac.groups().get("producers").clients()).containsExactly("alice");
        assertThat(rbac.groups().get("producers").roles()).containsExactly("producer");
        assertThat(rbac.groups().get("admins").clients()).containsExactly("bob");
        assertThat(rbac.groups().get("consumers").clients()).containsExactly("carol");
    }

    @Test
    void ignoresBootstrapFieldsOfOlderSnapshots() {
        // when
        DynamicConfig config = read("""
                {"listeners": [{"port": 9092}], "clusters": {"default": {"bootstrapServers": ["kafka:9092"]}},
                 "configTopic": "__kawa", "virtualTopics": {"orders": "orders-v2"}}
                """);

        // then
        assertThat(config.virtualTopics())
                .withFailMessage(() -> "A snapshot with leftover bootstrap fields was not read: " + config)
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    private DynamicConfig read(String json) {
        return mapper.readValue(json, DynamicConfig.class);
    }

    private VirtualTopicFilterConfig filterOf(String filterJson) {
        return read("""
                {"virtualTopics": {"customers": {"topic": "crm.customers", "filter": %s}}}
                """.formatted(filterJson.strip()))
                .virtualTopics().get("customers").filter();
    }
}
