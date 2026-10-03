package io.jonasg.kawa.core;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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
