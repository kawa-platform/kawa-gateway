package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// Fetch responses of a virtual topic whose configuration changed: every fetch answers per the
/// current configuration, and nothing prepared for a configuration no virtual topic has any more
/// - a filter, a value format - stays behind, however often the configuration changes.
class FetchVirtualTopicTransformReloadTest {

    private final GatewayContext context = new GatewayContext("consumer", System.nanoTime());

    @Test
    void filtersPerTheChangedFilterOnTheNextFetch() {
        // given a consumer that fetched `orders` while it kept values with a above 1
        var virtualTopics = new VirtualTopicManager(Map.of("orders", filtered("value.a > 1")));
        var transform = transform(virtualTopics);
        fetch(transform);

        // when its filter changes to keep values with a below 2
        virtualTopics.reload(Map.of("orders", filtered("value.a < 2")));
        List<String> values = values(fetch(transform));

        // then
        assertThat(values)
                .withFailMessage(() -> "The next fetch was not filtered per the changed filter, got " + values)
                .containsExactly("{\"a\":1}");
    }

    @Test
    void encodesInTheChangedOutputFormatOnTheNextFetch() {
        // given a consumer that fetched `orders` while its values were converted to XML rooted at <order>
        var virtualTopics = new VirtualTopicManager(Map.of("orders", convertedToXml("order")));
        var transform = transform(virtualTopics);
        fetch(transform);

        // when the root changes to <purchase>
        virtualTopics.reload(Map.of("orders", convertedToXml("purchase")));
        List<String> values = values(fetch(transform));

        // then
        assertThat(values)
                .withFailMessage(() -> "The next fetch was not encoded in the changed output format, got " + values)
                .containsExactly("<purchase><a>1</a></purchase>", "<purchase><a>2</a></purchase>", "<purchase><a>3</a></purchase>");
    }

    @Test
    void keepsNothingOfTheFilterOfARemovedVirtualTopic() {
        // given a consumer that fetched `orders` through its filter
        var virtualTopics = new VirtualTopicManager(Map.of());
        var transform = transform(virtualTopics);
        WeakReference<CelFilterConfig> filter = fetchThroughFilter(virtualTopics, transform);

        // when `orders` is removed
        virtualTopics.reload(Map.of());

        // then
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                .alias("The filter of the removed virtual topic is still kept")
                .until(() -> collected(filter));
    }

    @Test
    void keepsNothingOfTheOutputFormatAVirtualTopicNoLongerHas() {
        // given a consumer that fetched `orders` while its values were converted to XML rooted at <order>
        var virtualTopics = new VirtualTopicManager(Map.of());
        var transform = transform(virtualTopics);
        WeakReference<XmlFormatConfig> outputFormat = fetchConvertedToXml(virtualTopics, transform);

        // when the root changes to <purchase>, and the consumer fetches again
        virtualTopics.reload(Map.of("orders", convertedToXml("purchase")));
        fetch(transform);

        // then
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                .alias("The output format the virtual topic no longer has is still kept")
                .until(() -> collected(outputFormat));
    }

    /// Makes `orders` a filtered virtual topic and fetches it; only the returned reference is kept.
    private WeakReference<CelFilterConfig> fetchThroughFilter(VirtualTopicManager virtualTopics, FetchVirtualTopicTransform transform) {
        var filter = new CelFilterConfig("value.a > 1");
        virtualTopics.reload(Map.of("orders", new VirtualTopicConfig("orders.raw", filter, false, new JsonFormatConfig())));
        fetch(transform);
        return new WeakReference<>(filter);
    }

    /// Makes `orders` convert its values to XML rooted at <order> and fetches it; only the returned
    /// reference is kept.
    private WeakReference<XmlFormatConfig> fetchConvertedToXml(VirtualTopicManager virtualTopics, FetchVirtualTopicTransform transform) {
        var outputFormat = new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order");
        virtualTopics.reload(Map.of("orders", new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(),
                List.of(new ConvertTransformationConfig(outputFormat)), null)));
        fetch(transform);
        return new WeakReference<>(outputFormat);
    }

    private static boolean collected(WeakReference<?> reference) {
        System.gc();
        return reference.get() == null;
    }

    private static VirtualTopicConfig filtered(String expression) {
        return new VirtualTopicConfig("orders.raw", new CelFilterConfig(expression), false, new JsonFormatConfig());
    }

    private static VirtualTopicConfig convertedToXml(String root) {
        return new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(),
                List.of(new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, root))), null);
    }

    private static FetchVirtualTopicTransform transform(VirtualTopicManager virtualTopics) {
        return new FetchVirtualTopicTransform(virtualTopics, new FetchSessionRegistry());
    }

    /// Fetches partition 0 of `orders`; the broker answers with the values a = 1, 2 and 3.
    private MemoryRecords fetch(FetchVirtualTopicTransform transform) {
        var request = new FetchRequestData();
        request.topics().add(new FetchRequestData.FetchTopic().setTopic("orders"));
        transform.onRequest(context, request);

        var response = new FetchResponseData();
        response.responses().add(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders.raw")
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(3)
                        .setRecords(MemoryRecords.withRecords(Compression.NONE, json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}"))))));
        transform.onResponse(context, response);
        return (MemoryRecords) response.responses().getFirst().partitions().getFirst().records();
    }

    private static SimpleRecord json(String value) {
        return new SimpleRecord(value.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> values(MemoryRecords records) {
        List<String> values = new ArrayList<>();
        for (Record record : records.records()) {
            values.add(StandardCharsets.UTF_8.decode(record.value()).toString());
        }
        return values;
    }
}
