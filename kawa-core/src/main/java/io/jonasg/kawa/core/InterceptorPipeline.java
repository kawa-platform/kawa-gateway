package io.jonasg.kawa.core;

import io.jonasg.kawa.config.ModuleId;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Runs interceptors in registration order. Each interceptor either always runs or belongs to a
/// module; [#enable] is told which modules are switched on and swaps in the list of interceptors
/// to run, so a request only ever iterates interceptors that apply. The list is replaced as a
/// whole through a `volatile` field, never changed in place.
///
/// A request reads the list once per hook: one that is in flight while a module is switched may
/// run that module's `onRequest` without its `onResponse`, or the other way around. Switching a
/// module is a rare operator action, and that window is accepted.
public final class InterceptorPipeline {

    private final List<ModuleBinding> bindings;

    private volatile List<Interceptor> interceptors;

    /// A pipeline of interceptors that always run.
    public InterceptorPipeline(List<Interceptor> interceptors) {
        this.bindings = interceptors.stream().map(interceptor -> new ModuleBinding(null, interceptor)).toList();
        this.interceptors = List.copyOf(interceptors);
    }

    private InterceptorPipeline(Builder builder) {
        this.bindings = List.copyOf(builder.bindings);
        this.interceptors = this.bindings.stream().map(ModuleBinding::interceptor).toList();
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Runs only the interceptors that always run or whose module is in `modules`.
    public void enable(Set<ModuleId> modules) {
        this.interceptors = bindings.stream()
                .filter(binding -> binding.module() == null || modules.contains(binding.module()))
                .map(ModuleBinding::interceptor)
                .toList();
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

    /// An interceptor bound to the module it belongs to, or to `null` when it always runs.
    private record ModuleBinding(@Nullable ModuleId module, Interceptor interceptor) {
    }

    /// Declares the interceptors in the order they run.
    public static final class Builder {

        private final List<ModuleBinding> bindings = new ArrayList<>();

        private Builder() {
        }

        /// Adds an interceptor that always runs.
        public Builder always(Interceptor interceptor) {
            bindings.add(new ModuleBinding(null, interceptor));
            return this;
        }

        /// Adds an interceptor that only runs while `module` is enabled.
        public Builder module(ModuleId module, Interceptor interceptor) {
            bindings.add(new ModuleBinding(module, interceptor));
            return this;
        }

        /// A pipeline that runs every interceptor until [InterceptorPipeline#enable] says otherwise.
        public InterceptorPipeline build() {
            return new InterceptorPipeline(this);
        }
    }
}
