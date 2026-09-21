package io.jonasg.kawa.config;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    private static final String PLAIN_S3CRET = "pbkdf2-sha256$600000$000102030405060708090a0b0c0d0e0f$"
            + "9bb2521bd15ed9f43200646a7fc90af2f03f560b074ce7a3e1d1d85914c0494c";
    private static final String SCRAM_HUNTER2 = "scram-sha256$4096$000102030405060708090a0b0c0d0e0f$"
            + "8a399d90117a55a52e469133c5ef0ff0dc992b3b871a4a25b890556790c5e7e4$"
            + "9da30c0f6abfc8a7c371381f361a59aac6e4ce8450c0e3df4ff4e01c43d63fbc";

    private final ConfigLoader loader = new ConfigLoader();

    @Test
    void loadsFullConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                listeners:
                  - host: 0.0.0.0
                    port: 9092
                clusters:
                  default:
                    bootstrapServers:
                      - localhost:9093
                      - localhost:9094
                virtualTopics:
                  orders: orders-v2
                  customers:
                    topic: crm.customers
                advertised:
                  nodeId: 7
                  host: gw.example.com
                  port: 9092
                """);

        assertThat(config.listeners()).containsExactly(new ListenerConfig("0.0.0.0", 9092));
        ClusterConfig cluster = config.defaultCluster();
        assertThat(cluster.bootstrapServers()).containsExactly("localhost:9093", "localhost:9094");
        assertThat(config.virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"))
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
        assertThat(config.advertised()).isEqualTo(new AdvertisedListener(7, "gw.example.com", 9092));
    }

    @Test
    void appliesDefaults() {
        GatewayConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        assertThat(config.listeners()).containsExactly(new ListenerConfig("0.0.0.0", 9092));
        assertThat(config.clusters()).isEmpty();
        assertThat(config.virtualTopics()).isEmpty();
        assertThat(config.advertised()).isEqualTo(new AdvertisedListener(1, "localhost", 9092));
        assertThat(config.auth().mechanisms()).isEmpty();
        assertThat(config.auth().clients()).isEmpty();
        assertThat(config.auth().brokerAuth()).isNull();
        assertThat(config.admin().enabled()).isFalse();
        assertThat(config.configTopic()).isEqualTo("__kawa");
    }

    @Test
    void loadsConfigTopic() {
        GatewayConfig config = loader.loadFromYaml("""
                configTopic: kawa-config
                listeners:
                  - port: 9092
                """);

        assertThat(config.configTopic()).isEqualTo("kawa-config");
    }

    @Test
    void ignoresUnknownKeys() {
        GatewayConfig config = loader.loadFromYaml("""
                someFutureKey: 42
                listeners:
                  - port: 9092
                """);

        assertThat(config.listeners()).hasSize(1);
    }

    @Test
    void configurationIsImmutable() {
        GatewayConfig config = loader.loadFromYaml("virtualTopics:\n  orders: orders-v2\n");

        assertThatThrownBy(() -> config.virtualTopics().put("a", new VirtualTopicConfig("b")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidVirtualTopicMapping() {
        assertThatThrownBy(() -> loader.loadFromYaml("virtualTopics:\n  orders:\n    - not-a-topic-map\n"))
                .hasMessageContaining("Invalid virtual topic mapping for topic 'orders'");
    }

    @Test
    void loadsVirtualTopicObjectWithUnknownKeys() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    unknown: ignored
                """);

        assertThat(config.virtualTopics())
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
    }

    @Test
    void loadsVirtualTopicFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: headerEquals
                      header: tenant
                      value: acme
                """);

        assertThat(config.virtualTopics().get("customers").filter())
                .isEqualTo(new HeaderEqualsFilterConfig("tenant", "acme"));
    }

    @Test
    void loadsCelFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  orders:
                    topic: orders-v2
                    filter:
                      type: cel
                      expression: headers.tenant == "acme"
                """);

        assertThat(config.virtualTopics().get("orders").filter())
                .isEqualTo(new CelFilterConfig("headers.tenant == \"acme\""));
    }

    @Test
    void loadsHeaderContainsFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: headerContains
                      header: tenant
                      value: acm
                """);

        assertThat(config.virtualTopics().get("customers").filter())
                .isEqualTo(new HeaderContainsFilterConfig("tenant", "acm"));
    }

    @Test
    void loadsHeaderStartsWithFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: headerStartsWith
                      header: tenant
                      value: ac
                """);

        assertThat(config.virtualTopics().get("customers").filter())
                .isEqualTo(new HeaderStartsWithFilterConfig("tenant", "ac"));
    }

    @Test
    void loadsHeaderMatchesFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: headerMatches
                      header: tenant
                      value: eu.*
                """);

        assertThat(config.virtualTopics().get("customers").filter())
                .isEqualTo(new HeaderMatchesFilterConfig("tenant", "eu.*"));
    }

    @Test
    void rejectsInvalidHeaderMatchesRegex() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: headerMatches
                      header: tenant
                      value: a{2,1}
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'");
    }

    @Test
    void virtualTopicPhysicalNameIsHiddenByDefault() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  orders: orders-v2
                  customers:
                    topic: crm.customers
                """);

        assertThat(config.virtualTopics().get("orders").exposePhysicalTopic()).isFalse();
        assertThat(config.virtualTopics().get("customers").exposePhysicalTopic()).isFalse();
    }

    @Test
    void loadsExposePhysicalTopicOptIn() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  legacy:
                    topic: legacy-v1
                    exposePhysicalTopic: true
                """);

        assertThat(config.virtualTopics().get("legacy").exposePhysicalTopic()).isTrue();
    }

    @Test
    void rejectsNonBooleanExposePhysicalTopic() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    exposePhysicalTopic: "yes"
                """))
                .hasMessageContaining("exposePhysicalTopic");
    }

    @Test
    void rejectsVirtualTopicFilterWithoutType() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      header: tenant
                      value: acme
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'")
                .hasMessageContaining("type");
    }

    @Test
    void rejectsUnknownVirtualTopicFilterType() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: notARealType
                      header: tenant
                      value: acme
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'")
                .hasMessageContaining("type");
    }

    @Test
    void loadsFullAuthConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  mechanisms:
                    - PLAIN
                    - SCRAM-SHA-256
                  clients:
                    alice:
                      password: %s
                    bob:
                      mechanism: SCRAM-SHA-256
                      password: %s
                listeners:
                  - port: 9092
                """.formatted(PLAIN_S3CRET, SCRAM_HUNTER2));

        assertThat(config.auth().mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
        assertThat(config.auth().clients()).hasSize(2);
        assertThat(config.auth().clients().get("alice").mechanism()).isEqualTo("PLAIN");
        assertThat(config.auth().clients().get("alice").password().verify("s3cret"))
                .withFailMessage(() -> "Loaded PLAIN password did not verify against the original plaintext")
                .isTrue();
        assertThat(config.auth().clients().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
        assertThat(config.auth().clients().get("bob").password().encoded())
                .isEqualTo(SCRAM_HUNTER2);
    }

    @Test
    void rejectsClientWithoutMechanismWhenNoGlobalMechanism() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                auth:
                  clients:
                    alice:
                      password: pbkdf2-sha256$600000$000102030405060708090a0b0c0d0e0f$9bb2521bd15ed9f43200646a7fc90af2f03f560b074ce7a3e1d1d85914c0494c
                listeners:
                  - port: 9092
                """))
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void authConfigurationIsImmutable() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  mechanisms:
                    - PLAIN
                  clients:
                    alice:
                      password: pbkdf2-sha256$600000$000102030405060708090a0b0c0d0e0f$9bb2521bd15ed9f43200646a7fc90af2f03f560b074ce7a3e1d1d85914c0494c
                listeners:
                  - port: 9092
                """);

        assertThatThrownBy(() -> config.auth().mechanisms().add("SCRAM-SHA-512"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.auth().clients().put("bob",
                new ClientConfig("PLAIN", HashedPassword.fromPlaintext(Mechanism.PLAIN, "pw"))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void loadsBrokerAuthConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  brokerAuth:
                    mechanism: PLAIN
                    username: gateway-service
                    password: s3cret
                listeners:
                  - port: 9092
                """);

        assertThat(config.auth().brokerAuth()).isNotNull();
        assertThat(config.auth().brokerAuth().mechanism()).isEqualTo("PLAIN");
        assertThat(config.auth().brokerAuth().username()).isEqualTo("gateway-service");
        assertThat(config.auth().brokerAuth().password()).isEqualTo("s3cret");
    }

    @Test
    void brokerAuthDefaultIsNull() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  mechanisms:
                    - PLAIN
                  clients:
                    alice:
                      password: pbkdf2-sha256$600000$000102030405060708090a0b0c0d0e0f$40595f52de533962fe67dfc916702017baa23908e1e26ea122766e9215bcd403
                listeners:
                  - port: 9092
                """);

        assertThat(config.auth().brokerAuth()).isNull();
    }

    @Test
    void loadsRbacConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                rbac:
                  roles:
                    producer:
                      acls:
                        - resource:
                            type: TOPIC
                            pattern: orders
                            patternType: LITERAL
                          operation: WRITE
                          permission: ALLOW
                    admin:
                      acls:
                        - resource:
                            type: CLUSTER
                          operation: CREATE
                    consumer:
                      acls:
                        - resource:
                            type: GROUP
                            pattern: orders-group
                          operation: READ
                  groups:
                    producers:
                      clients: [alice]
                      roles: [producer]
                    admins:
                      clients: [bob]
                      roles: [admin]
                    consumers:
                      clients: [carol]
                      roles: [consumer]
                """);

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
    void loadsGovernanceConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                governance:
                  topicRules:
                    min-partitions:
                      message: must have at least one partition
                      expression: topic.partitions >= 1
                    max-partitions:
                      message: too many partitions
                      expression: topic.partitions <= 12
                  exemptions:
                    streams-internal:
                      principal: ^streams-.*
                      topicPattern: .*-changelog$
                listeners:
                  - port: 9092
                """);

        assertThat(config.governance().topicRules()).containsEntry("min-partitions",
                new GovernanceRuleConfig("must have at least one partition", "topic.partitions >= 1"));
        assertThat(config.governance().topicRules()).containsEntry("max-partitions",
                new GovernanceRuleConfig("too many partitions", "topic.partitions <= 12"));
        assertThat(config.governance().exemptions()).containsEntry("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));
    }

    @Test
    void governanceDefaultsToEmpty() {
        GatewayConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        assertThat(config.governance().topicRules()).isEmpty();
        assertThat(config.governance().exemptions()).isEmpty();
    }

    @Test
    void loadsAdminConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  host: 127.0.0.1
                  port: 8081
                """);

        assertThat(config.admin().enabled()).isTrue();
        assertThat(config.admin().host()).isEqualTo("127.0.0.1");
        assertThat(config.admin().port()).isEqualTo(8081);
        assertThat(config.admin().cors()).isNull();
    }

    @Test
    void loadsAdminCorsConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  host: 127.0.0.1
                  port: 8081
                  cors:
                    allowedOrigins:
                      - http://localhost:8080
                      - http://localhost:5173
                    allowedMethods:
                      - GET
                      - OPTIONS
                    allowedHeaders:
                      - Content-Type
                    allowCredentials: true
                    maxAge: 3600
                """);

        CorsConfig cors = config.admin().cors();
        assertThat(cors).isNotNull();
        assertThat(cors.allowedOrigins())
                .containsExactly("http://localhost:8080", "http://localhost:5173");
        assertThat(cors.allowedMethods()).containsExactly("GET", "OPTIONS");
        assertThat(cors.allowedHeaders()).containsExactly("Content-Type");
        assertThat(cors.allowCredentials()).isTrue();
        assertThat(cors.maxAge()).isEqualTo(3600L);
    }

    @Test
    void adminCorsDefaultsToDisabled() {
        GatewayConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        assertThat(config.admin().enabled()).isFalse();
        assertThat(config.admin().host()).isEqualTo("0.0.0.0");
        assertThat(config.admin().port()).isEqualTo(8080);
        assertThat(config.admin().cors()).isNull();
    }
}
