package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CachingAvroSchemaResolverTest {

    private static final Schema STRING = Schema.create(Schema.Type.STRING);
    private static final Schema LONG = Schema.create(Schema.Type.LONG);

    @Test
    void resolvesEachSchemaIdOnce() {
        // given
        List<Integer> lookups = Collections.synchronizedList(new ArrayList<>());
        var resolver = new CachingAvroSchemaResolver(id -> {
            lookups.add(id);
            return id == 1 ? STRING : LONG;
        });

        // when
        Schema first = resolver.resolve(1);
        Schema again = resolver.resolve(1);
        Schema other = resolver.resolve(2);

        // then
        assertThat(lookups)
                .withFailMessage(() -> "Each schema id was not looked up exactly once, lookups were " + lookups)
                .containsExactly(1, 2);
        assertThat(again)
                .withFailMessage(() -> "The cached schema for id 1 was not the one first resolved")
                .isSameAs(first);
        assertThat(other)
                .withFailMessage(() -> "Schema id 2 did not resolve to its own schema")
                .isEqualTo(LONG);
    }

    @Test
    void remembersAFailedLookupForTheFailureTtl() {
        // given a registry that does not know id 7
        List<Integer> lookups = new ArrayList<>();
        var clock = new AtomicLong();
        var resolver = new CachingAvroSchemaResolver(id -> {
            lookups.add(id);
            throw new IllegalStateException("schema " + id + " not found");
        }, Duration.ofSeconds(5), clock::get);

        // when looking it up for every record within the failure ttl
        for (int i = 0; i < 3; i++) {
            clock.addAndGet(Duration.ofSeconds(1).toNanos());
            assertThatThrownBy(() -> resolver.resolve(7))
                    .withFailMessage(() -> "A lookup of unknown schema id 7 within the failure ttl did not fail")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("schema 7 not found");
        }

        // then the registry was asked only once
        assertThat(lookups)
                .withFailMessage(() -> "The failed lookup was not remembered, the registry was asked " + lookups.size() + " times")
                .containsExactly(7);
    }

    @Test
    void retriesAFailedLookupOnceTheFailureTtlHasPassed() {
        // given a registry that registers id 7 after the first lookup failed
        List<Integer> lookups = new ArrayList<>();
        var clock = new AtomicLong();
        var resolver = new CachingAvroSchemaResolver(id -> {
            lookups.add(id);
            if (lookups.size() == 1) {
                throw new IllegalStateException("schema " + id + " not found");
            }
            return STRING;
        }, Duration.ofSeconds(5), clock::get);
        assertThatThrownBy(() -> resolver.resolve(7))
                .withFailMessage(() -> "The failing first lookup did not propagate its error")
                .isInstanceOf(IllegalStateException.class);

        // when looking it up again after the failure ttl
        clock.addAndGet(Duration.ofSeconds(5).toNanos());
        Schema schema = resolver.resolve(7);

        // then
        assertThat(schema)
                .withFailMessage(() -> "Schema id 7 was not resolved once the failure ttl had passed")
                .isEqualTo(STRING);
        assertThat(lookups)
                .withFailMessage(() -> "The failed lookup was not retried after the failure ttl, lookups were " + lookups)
                .containsExactly(7, 7);
    }

    @Test
    void aFailedIdDoesNotBlockOtherIds() {
        // given id 7 failed
        var resolver = new CachingAvroSchemaResolver(id -> {
            if (id == 7) {
                throw new IllegalStateException("schema 7 not found");
            }
            return LONG;
        }, Duration.ofSeconds(5), () -> 0L);
        assertThatThrownBy(() -> resolver.resolve(7))
                .withFailMessage(() -> "The failing lookup of id 7 did not propagate its error")
                .isInstanceOf(IllegalStateException.class);

        // when
        Schema other = resolver.resolve(8);

        // then
        assertThat(other)
                .withFailMessage(() -> "Schema id 8 was not resolved while id 7 was remembered as failed")
                .isEqualTo(LONG);
    }
}
