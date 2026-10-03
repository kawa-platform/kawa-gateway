package io.jonasg.kawa.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class InterceptorPipeline {

    private final List<Interceptor> interceptors;

    public InterceptorPipeline(List<Interceptor> interceptors) {
        this.interceptors = List.copyOf(interceptors);
    }

    /// Starts every applicable interceptor's [Interceptor#prepare] lookup.
    ///
    /// @return a stage completing once all of them have (successfully or not), or `null` when
    ///         no interceptor has anything to wait for, so the common path stays synchronous
    public CompletionStage<Void> prepare(
            GatewayContext context,
            Request request
    ) {
        List<CompletableFuture<?>> pending = null;
        for (Interceptor interceptor : interceptors) {
            if (interceptor.appliesToRequest(request)) {
                CompletionStage<?> stage = interceptor.prepare(context, request);
                if (stage != null) {
                    if (pending == null) {
                        pending = new ArrayList<>();
                    }
                    pending.add(stage.toCompletableFuture());
                }
            }
        }
        if (pending == null) {
            return null;
        }
        return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                .handle((_, _) -> null);
    }

    public void onRequest(
            GatewayContext context,
            Request request
    ) {
        for (Interceptor interceptor : interceptors) {
            if (interceptor.appliesToRequest(request)) {
                interceptor.onRequest(context, request);
                if (context.isShortCircuited()) {
                    return;
                }
            }
        }
    }

    public void onResponse(
            GatewayContext context,
            Response response
    ) {
        for (Interceptor interceptor : interceptors) {
            if (interceptor.appliesToResponse(response)) {
                interceptor.onResponse(context, response);
            }
        }
    }
}
