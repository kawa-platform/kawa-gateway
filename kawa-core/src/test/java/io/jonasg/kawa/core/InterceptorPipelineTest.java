package io.jonasg.kawa.core;

import io.jonasg.kawa.config.ModuleId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

class InterceptorPipelineTest {

    private final List<String> events = new ArrayList<>();

    @Test
    void invokesInterceptorsInOrderForBothHooks() {
        Interceptor first = new RecordingInterceptor("first");
        Interceptor second = new RecordingInterceptor("second");
        var pipeline = new InterceptorPipeline(List.of(first, second));

        var ctx = new GatewayContext("source", 0L);
        pipeline.onRequest(ctx, null);
        pipeline.onResponse(ctx, null);

        assertThat(events).containsExactly(
                "first.onRequest", "second.onRequest",
                "first.onResponse", "second.onResponse");
    }

    @Test
    void contextCarriesRouteDecision() {
        var pipeline = new InterceptorPipeline(List.of(new Interceptor() {
            @Override
            public void onRequest(
                    GatewayContext context,
                    Request request
            ) {
                context.route(Route.to(42));
            }
        }));

        var ctx = new GatewayContext("source", 0L);
        pipeline.onRequest(ctx, null);

        assertThat(ctx.route().brokerId()).isEqualTo(42);
    }

    @Test
    void skipsInterceptorsWhenApplicabilityReturnsFalse() {
        var skipped = new Interceptor() {
            @Override
            public boolean appliesToRequest(Request request) {
                return false;
            }

            @Override
            public boolean appliesToResponse(Response response) {
                return false;
            }

            @Override
            public void onRequest(
                    GatewayContext context,
                    Request request
            ) {
                events.add("skipped.onRequest");
            }

            @Override
            public void onResponse(
                    GatewayContext context,
                    Response response
            ) {
                events.add("skipped.onResponse");
            }
        };
        Interceptor active = new RecordingInterceptor("active");
        var pipeline = new InterceptorPipeline(List.of(skipped, active));

        var ctx = new GatewayContext("source", 0L);
        pipeline.onRequest(ctx, null);
        pipeline.onResponse(ctx, null);

        assertThat(events).containsExactly("active.onRequest", "active.onResponse");
    }

    @Test
    void stopsInvokingFurtherInterceptorsOnceOneShortCircuitsTheRequest() {
        Interceptor shortCircuiting = new Interceptor() {
            @Override
            public void onRequest(GatewayContext context, Request request) {
                events.add("shortCircuiting.onRequest");
                context.shortCircuit(new ShortCircuitResult((short) 0, (short) 0, "body"));
            }
        };
        Interceptor never = new RecordingInterceptor("never");
        var pipeline = new InterceptorPipeline(List.of(shortCircuiting, never));

        var ctx = new GatewayContext("source", 0L);
        pipeline.onRequest(ctx, null);

        assertThat(events).containsExactly("shortCircuiting.onRequest");
        assertThat(ctx.isShortCircuited()).isTrue();
    }

    @Test
    void prepareIsNullWhenNoInterceptorHasALookup() {
        // given
        var pipeline = new InterceptorPipeline(List.of(new RecordingInterceptor("first")));

        // when
        var prepared = pipeline.prepare(new GatewayContext("source", 0L), null);

        // then
        assertThat(prepared)
                .withFailMessage(() -> "A request without lookups was not kept on the synchronous path")
                .isNull();
    }

    @Test
    void prepareCompletesOnceEveryLookupHasEvenAFailedOne() {
        // given
        var slow = new CompletableFuture<String>();
        var failed = CompletableFuture.<String>failedFuture(new IllegalStateException("broker down"));
        var pipeline = new InterceptorPipeline(List.of(lookup(slow), lookup(failed)));

        // when
        var prepared = pipeline.prepare(new GatewayContext("source", 0L), null).toCompletableFuture();

        // then
        assertThat(prepared.isDone()).isFalse();
        slow.complete("done");
        assertThat(prepared)
                .withFailMessage(() -> "A failed lookup failed the pipeline instead of being left to onRequest")
                .isCompletedWithValue(null);
    }

    @Test
    void runsEveryInterceptorUntilToldWhichModulesAreEnabled() {
        // given
        var pipeline = InterceptorPipeline.builder()
                .always(new RecordingInterceptor("always"))
                .module(ModuleId.VIRTUAL_TOPICS, new RecordingInterceptor("virtual-topics"))
                .build();

        // when
        pipeline.onRequest(new GatewayContext("source", 0L), null);

        // then
        assertThat(events)
                .withFailMessage(() -> "A freshly built pipeline skipped an interceptor: " + events)
                .containsExactly("always.onRequest", "virtual-topics.onRequest");
    }

    @Test
    void skipsTheInterceptorsOfADisabledModule() {
        // given
        var pipeline = InterceptorPipeline.builder()
                .always(new RecordingInterceptor("first"))
                .module(ModuleId.VIRTUAL_TOPICS, new RecordingInterceptor("virtual-topics"))
                .always(new RecordingInterceptor("last"))
                .build();
        var ctx = new GatewayContext("source", 0L);

        // when
        pipeline.enable(Set.of(ModuleId.RBAC, ModuleId.GOVERNANCE));
        pipeline.onRequest(ctx, null);
        pipeline.onResponse(ctx, null);

        // then
        assertThat(events)
                .withFailMessage(() -> "Disabled module 'virtual-topics' still ran: " + events)
                .containsExactly("first.onRequest", "last.onRequest", "first.onResponse", "last.onResponse");
    }

    @Test
    void runsAModuleAgainInItsPlaceOnceItIsReEnabled() {
        // given
        var pipeline = InterceptorPipeline.builder()
                .always(new RecordingInterceptor("first"))
                .module(ModuleId.VIRTUAL_TOPICS, new RecordingInterceptor("virtual-topics"))
                .always(new RecordingInterceptor("last"))
                .build();
        pipeline.enable(Set.of());

        // when
        pipeline.enable(Set.of(ModuleId.VIRTUAL_TOPICS));
        pipeline.onRequest(new GatewayContext("source", 0L), null);

        // then
        assertThat(events)
                .withFailMessage(() -> "Re-enabled module 'virtual-topics' did not run in its declared place: " + events)
                .containsExactly("first.onRequest", "virtual-topics.onRequest", "last.onRequest");
    }

    @Test
    void doesNotStartTheLookupOfADisabledModule() {
        // given
        var pipeline = InterceptorPipeline.builder()
                .module(ModuleId.GOVERNANCE, lookup(new CompletableFuture<>()))
                .build();
        pipeline.enable(Set.of());

        // when
        var prepared = pipeline.prepare(new GatewayContext("source", 0L), null);

        // then
        assertThat(prepared)
                .withFailMessage(() -> "Disabled module 'governance' started a lookup the request waits on")
                .isNull();
    }

    private static Interceptor lookup(CompletionStage<?> stage) {
        return new Interceptor() {
            @Override
            public CompletionStage<?> prepare(GatewayContext context, Request request) {
                return stage;
            }
        };
    }

    private final class RecordingInterceptor implements Interceptor {
        private final String name;

        private RecordingInterceptor(String name) {
            this.name = name;
        }

        @Override
        public void onRequest(
                GatewayContext context,
                Request request
        ) {
            events.add(name + ".onRequest");
        }

        @Override
        public void onResponse(
                GatewayContext context,
                Response response
        ) {
            events.add(name + ".onResponse");
        }
    }
}
