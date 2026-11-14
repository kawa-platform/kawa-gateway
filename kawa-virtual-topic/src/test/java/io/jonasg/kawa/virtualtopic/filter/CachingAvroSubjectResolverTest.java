package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CachingAvroSubjectResolverTest {

    private static final RegisteredAvroSchema V1 = new RegisteredAvroSchema(1, Schema.create(Schema.Type.STRING));
    private static final RegisteredAvroSchema V2 = new RegisteredAvroSchema(2, Schema.create(Schema.Type.LONG));

    private final AtomicLong clock = new AtomicLong();
    private final List<String> lookups = new ArrayList<>();

    @Test
    void cachesAnExplicitVersionForGood() {
        // given
        var resolver = resolver((subject, version) -> V1);

        // when
        resolver.resolve("orders-value", 1);
        clock.addAndGet(Duration.ofDays(1).toNanos());
        RegisteredAvroSchema again = resolver.resolve("orders-value", 1);

        // then
        assertThat(lookups)
                .withFailMessage(() -> "An explicit version was looked up more than once: " + lookups)
                .containsExactly("orders-value@1");
        assertThat(again).isEqualTo(V1);
    }

    @Test
    void looksUpTheLatestVersionAgainAfterItsTtl() {
        // given a registry where a new version is registered after the first lookup
        var registered = new ArrayList<>(List.of(V1, V2));
        var resolver = resolver((subject, version) -> registered.removeFirst());

        // when
        RegisteredAvroSchema first = resolver.resolve("orders-value", null);
        clock.addAndGet(Duration.ofMinutes(4).toNanos());
        RegisteredAvroSchema withinTtl = resolver.resolve("orders-value", null);
        clock.addAndGet(Duration.ofMinutes(2).toNanos());
        RegisteredAvroSchema afterTtl = resolver.resolve("orders-value", null);

        // then
        assertThat(List.of(first, withinTtl, afterTtl))
                .withFailMessage(() -> "The latest version was not cached within its ttl and refreshed after it: " + lookups)
                .containsExactly(V1, V1, V2);
    }

    @Test
    void remembersAFailedLookupForTheFailureTtl() {
        // given
        var resolver = resolver((subject, version) -> {
            throw new IllegalStateException("subject " + subject + " not found");
        });

        // when
        assertThatThrownBy(() -> resolver.resolve("nope", null)).hasMessageContaining("not found");
        clock.addAndGet(Duration.ofSeconds(1).toNanos());
        assertThatThrownBy(() -> resolver.resolve("nope", null)).hasMessageContaining("not retried yet");
        clock.addAndGet(Duration.ofSeconds(5).toNanos());
        assertThatThrownBy(() -> resolver.resolve("nope", null)).hasMessageContaining("not found");

        // then
        assertThat(lookups)
                .withFailMessage(() -> "The registry was asked again within the failure ttl, or not after it: " + lookups)
                .containsExactly("nope@latest", "nope@latest");
    }

    private CachingAvroSubjectResolver resolver(AvroSubjectResolver registry) {
        AvroSubjectResolver recording = (subject, version) -> {
            lookups.add(subject + "@" + (version == null ? "latest" : version));
            return registry.resolve(subject, version);
        };
        return new CachingAvroSubjectResolver(recording, Duration.ofMinutes(5), Duration.ofSeconds(5), clock::get);
    }
}
