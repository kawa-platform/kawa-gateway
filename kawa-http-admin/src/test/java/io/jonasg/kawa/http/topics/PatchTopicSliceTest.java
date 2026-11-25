package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeDynamicConfigRepository;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for `PATCH /topics/{name}`: a field the patch omits keeps its current value.
class PatchTopicSliceTest extends AdminHttpSliceTestBase {

    @Test
    void renameKeepsValueFormatTransformationsAndFilter() throws Exception {
        // given
        var filter = new HeaderEqualsFilterConfig("region", "eu");
        var steps = List.<TransformationConfig>of(new DropFieldsTransformationConfig(List.of("secret")));
        var current = new VirtualTopicConfig("orders", filter, false, new JsonFormatConfig(), steps, null);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders.public", """
                {"name": "renamed"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        var renamed = repository.getActiveConfig().virtualTopics().get("renamed");
        assertThat(renamed)
                .withFailMessage(() -> "A rename did not keep the omitted valueFormat, transformations and filter: " + renamed)
                .isEqualTo(current);
    }

    @Test
    void renameKeepsFilterAndValueFormat() throws Exception {
        // given
        var filter = new HeaderEqualsFilterConfig("region", "eu");
        var current = new VirtualTopicConfig("orders", filter, false, new JsonFormatConfig(), List.of(), null);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders.public", """
                {"name": "renamed"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        var renamed = repository.getActiveConfig().virtualTopics().get("renamed");
        assertThat(renamed)
                .withFailMessage(() -> "A rename did not keep the omitted filter and valueFormat: " + renamed)
                .isEqualTo(current);
    }

    @Test
    void patchesVirtualTopicConfiguration() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1")));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders",
                """
                        {
                          "topic": "orders-v2",
                          "filter": {
                            "type": "headerEquals",
                            "header": "region",
                            "value": "eu"
                          },
                          "exposePhysicalTopic": false
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2",
                        new HeaderEqualsFilterConfig("region", "eu"), false));
        assertThatJson(response.body()).isEqualTo("""
                {
                  "topic": "orders-v2",
                  "filter": {
                    "type": "headerEquals",
                    "header": "region",
                    "value": "eu"
                  },
                  "exposePhysicalTopic": false,
                  "valueFormat": null,
                  "transformations": [],
                  "managedBy": null
                }
                """);
    }

    @Test
    void preservesOmittedVirtualTopicFieldsWhenPatching() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders-v1", null, true);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2", null, true));
    }

    @Test
    void clearsFilterWhenFilterIsNull() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1",
                        new HeaderEqualsFilterConfig("region", "eu"), true)));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"filter\":null}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v1", null, true));
    }

    @Test
    void keepsFilterWhenFilterIsOmitted() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1",
                        new HeaderEqualsFilterConfig("region", "eu"), true)));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"exposePhysicalTopic\":false}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v1",
                        new HeaderEqualsFilterConfig("region", "eu"), false));
    }

    @Test
    void renamesVirtualTopicWithoutCreatingAnotherEntry() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders-v1", null, true);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"name\":\"regional-orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsOnlyKeys("regional-orders")
                .containsEntry("regional-orders", current);
    }

    @Test
    void rejectsPatchForMissingVirtualTopic() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }

    @Test
    void rejectsPatchingPhysicalTopic() throws Exception {
        // given
        cache = cacheWith(topic("orders", 1, 1));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }

    @Test
    void rejectsPatchWhenRenamedVirtualTopicAlreadyExists() throws Exception {
        // given
        var original = new VirtualTopicConfig("orders-v1");
        var existing = new VirtualTopicConfig("customers-v1");
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", original)
                .upsertVirtualTopic("customers", existing));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"name\":\"customers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", original)
                .containsEntry("customers", existing);
    }

    @Test
    void rejectsInvalidPatchWithoutChangingExistingConfiguration() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders-v1");
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders",
                "{\"topic\":\"\",\"filter\":{\"type\":\"unknown\"}}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", current);
    }

    @Test
    void patchResponseOmitsNameAndType() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1")));
        startServer();

        // when
        var patchResp = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(patchResp.statusCode()).isEqualTo(200);
        assertThat(patchResp).containsExactlyTopLevelPropertyNames("topic", "filter", "exposePhysicalTopic", "valueFormat", "transformations", "managedBy");
    }

    @Test
    void patchKeepsTransformationsWhenOmittedAndRemovesThemWhenEmpty() throws Exception {
        // given
        var steps = List.<TransformationConfig>of(new DropFieldsTransformationConfig(List.of("secret")));
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public",
                new VirtualTopicConfig("orders", null, false, new JsonFormatConfig(), steps, null)));
        startServer();

        // when
        var keepResp = send("PATCH", "/topics/orders.public", """
                {"exposePhysicalTopic": true, "valueFormat": {"type": "json"}}
                """);
        var kept = repository.getActiveConfig().virtualTopics().get("orders.public").transformations();
        var replaceResp = send("PATCH", "/topics/orders.public", """
                {"valueFormat": {"type": "json"}, "transformations": [{"type": "renameField", "from": "a", "to": "b"}]}
                """);
        var replaced = repository.getActiveConfig().virtualTopics().get("orders.public").transformations();
        var removeResp = send("PATCH", "/topics/orders.public", """
                {"valueFormat": {"type": "json"}, "transformations": []}
                """);

        // then
        assertThat(keepResp).hasStatusCode(200);
        assertThat(kept)
                .withFailMessage(() -> "A PATCH without transformations did not keep the steps: " + kept)
                .isEqualTo(steps);
        assertThat(replaceResp).hasStatusCode(200);
        assertThat(replaced)
                .withFailMessage(() -> "A PATCH with a list did not replace the steps: " + replaced)
                .containsExactly(new RenameFieldTransformationConfig("a", "b"));
        assertThat(removeResp).hasStatusCode(200);
        assertThat(repository.getActiveConfig().virtualTopics().get("orders.public").transformations())
                .withFailMessage(() -> "A PATCH with [] did not remove the steps")
                .isEmpty();
    }

    @Test
    void patchesHeaderStepsOntoAnAvroTopicWithoutASubject() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public",
                new VirtualTopicConfig("orders", null, false, new AvroFormatConfig(), List.of(), null)));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"transformations": [
                  {"type": "dropHeaders", "headers": ["__TypeId__"]},
                  {"type": "setHeader", "header": "content-type", "value": "application/avro"}
                ]}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(200);
        var patched = repository.getActiveConfig().virtualTopics().get("orders.public").transformations();
        assertThat(patched)
                .withFailMessage(() -> "The header steps were not patched onto the Avro topic without a subject: " + patched)
                .containsExactly(
                        new DropHeadersTransformationConfig(List.of("__TypeId__")),
                        new SetHeaderTransformationConfig("content-type", "application/avro"));
    }

    @Test
    void patchesHeaderStepsOntoAnXmlTopicWithoutARoot() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public",
                new VirtualTopicConfig("orders", null, false, new XmlFormatConfig(), List.of(), null)));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"transformations": [
                  {"type": "dropHeaders", "headers": ["__TypeId__"]},
                  {"type": "setHeader", "header": "content-type", "value": "application/xml"}
                ]}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(200);
        var patched = repository.getActiveConfig().virtualTopics().get("orders.public").transformations();
        assertThat(patched)
                .withFailMessage(() -> "The header steps were not patched onto the XML topic without a root: " + patched)
                .containsExactly(
                        new DropHeadersTransformationConfig(List.of("__TypeId__")),
                        new SetHeaderTransformationConfig("content-type", "application/xml"));
    }

    @Test
    void refusesAPatchThatDropsTheValueFormatTheStepsNeed() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public",
                new VirtualTopicConfig("orders", null, false, new JsonFormatConfig(),
                        List.of(new DropFieldsTransformationConfig(List.of("secret"))), null)));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"valueFormat": null}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(400);
        assertThatJson(patchTopicResp.body()).isEqualTo("""
                {"error": "value transformations and convert require a 'valueFormat' to decode the record value"}
                """);
    }

    @Test
    void refusesAPatchedStepWithAnUnknownKeyWithoutChangingTheSteps() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders", null, false, new JsonFormatConfig(),
                List.of(new DropFieldsTransformationConfig(List.of("secret"))), null);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public", current));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"transformations": [{"type": "renameField", "from": "a", "to": "b", "form": "c"}]}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(400);
        assertThatJson(patchTopicResp.body()).isEqualTo("""
                {"error": "transformations[0] (renameField): unknown property 'form'"}
                """);
        assertThat(repository.getActiveConfig().virtualTopics().get("orders.public"))
                .withFailMessage(() -> "A refused patch changed the virtual topic")
                .isEqualTo(current);
    }

    @Test
    void refusesAPatchedConvertThatIsNotTheLastStepNamingIt() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public",
                new VirtualTopicConfig("orders", null, false, new JsonFormatConfig(), List.of(), null)));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"transformations": [
                  {"type": "dropFields", "fields": ["a"]},
                  {"type": "convert", "format": {"type": "json"}},
                  {"type": "dropFields", "fields": ["b"]}
                ]}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(400);
        assertThatJson(patchTopicResp.body()).isEqualTo("""
                {"error": "transformations[1] (convert): only header steps may follow it"}
                """);
    }

    @Test
    void refusesAPatchToAvroWithoutASchemaRegistry() throws Exception {
        // given a JSON virtual topic on a cluster without a schema registry
        schemaRegistries = SchemaRegistries.NONE;
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public",
                new VirtualTopicConfig("orders", null, false, new JsonFormatConfig(),
                        List.of(new DropFieldsTransformationConfig(List.of("secret"))), null)));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"valueFormat": {"type": "avro"}}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(400);
        assertThat(repository.getActiveConfig().virtualTopics().get("orders.public").valueFormat())
                .withFailMessage(() -> "The value format was switched to Avro without a schema registry")
                .isEqualTo(new JsonFormatConfig());
    }

    @Test
    void rejectsAnInvalidFilterWithoutChangingExistingConfiguration() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders", new HeaderEqualsFilterConfig("region", "eu"), true);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertVirtualTopic("orders.public", current));
        startServer();

        // when
        var patchTopicResp = send("PATCH", "/topics/orders.public", """
                {"filter": {"type": "unknown"}}
                """);

        // then
        assertThat(patchTopicResp).hasStatusCode(400);
        assertThat(patchTopicResp.body())
                .withFailMessage(() -> "The 400 for an invalid filter did not say the topic body is invalid: " + patchTopicResp.body())
                .contains("invalid topic body: Could not resolve type id 'unknown'");
        assertThat(repository.getActiveConfig().virtualTopics())
                .withFailMessage(() -> "An invalid filter changed the stored virtual topic")
                .containsEntry("orders.public", current);
    }
}
