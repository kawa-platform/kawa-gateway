package io.jonasg.kawa.protocol.kafka;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ApiVersionsResponseBuilderTest {

    private final KafkaApiRegistry registry = KafkaApiRegistry.create();
    private final ApiVersionsResponseBuilder builder =
            new ApiVersionsResponseBuilder(SupportedVersions.from(registry));

    private static ApiVersionsResponseData.ApiVersion find(
            ApiVersionsResponseData data, short apiKey) {
        return data.apiKeys().stream()
                .filter(a -> a.apiKey() == apiKey)
                .findFirst()
                .orElseThrow();
    }

    @Test
    void intersectsDecodedRangesWithBrokerRanges() {
        Map<Short, VersionRange> broker = Map.of(
                (short) 18, VersionRange.of((short) 0, (short) 3),
                (short) 0, VersionRange.of((short) 0, (short) 13),
                (short) 3, VersionRange.of((short) 0, (short) 12),
                (short) 8, VersionRange.of((short) 0, (short) 8),
                (short) 50, VersionRange.of((short) 0, (short) 9));

        var result = builder.build((short) 3, broker);

        assertThat(result.responseVersion()).isEqualTo((short) 3);
        assertThat(result.data().errorCode()).isZero();

        ApiVersionsResponseData.ApiVersion metadata = find(result.data(), (short) 3);
        assertThat(metadata.maxVersion()).isEqualTo((short) 8); // our max, not broker 12

        ApiVersionsResponseData.ApiVersion produce = find(result.data(), (short) 0);
        assertThat(produce.maxVersion()).isEqualTo((short) 12); // our max, not broker 13 (topic ids)

        ApiVersionsResponseData.ApiVersion offsetCommit = find(result.data(), (short) 8);
        assertThat(offsetCommit.maxVersion()).isEqualTo((short) 7); // broker caps at 7

        // DescribeUserScramCredentials (50) is not registered
        ApiVersionsResponseData.ApiVersion passthrough = find(result.data(), (short) 50);
        assertThat(passthrough.maxVersion()).isEqualTo((short) 9); // passthrough -> broker range
    }

    @Test
    void fallsBackToDecodeRangesWhenBrokerRangesUnknown() {
        var result = builder.build((short) 3, Map.of());

        ApiVersionsResponseData.ApiVersion fetch = find(result.data(), (short) 1);
        assertThat(fetch.maxVersion()).isEqualTo((short) 11);
    }

    @Test
    void reportsUnsupportedVersionForTooNewRequest() {
        // Client asks for ApiVersions v4, gateway supports only up to v3
        Map<Short, VersionRange> broker = Map.of(
                (short) 18, VersionRange.of((short) 0, (short) 3),
                (short) 3, VersionRange.of((short) 0, (short) 12));

        var result = builder.build((short) 4, broker);

        assertThat(result.responseVersion()).isEqualTo((short) 3); // highest supported
        assertThat(result.data().errorCode()).isNotZero();
        assertThat(result.data().apiKeys()).isNotEmpty();
    }

    @Test
    void respondsToOlderVersions() {
        var result = builder.build((short) 0, Map.of());

        assertThat(result.responseVersion()).isZero();
        assertThat(result.data().errorCode()).isZero();
    }
}
