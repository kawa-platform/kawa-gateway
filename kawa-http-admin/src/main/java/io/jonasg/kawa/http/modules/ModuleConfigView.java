package io.jonasg.kawa.http.modules;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

/// A module in an admin `/modules` response.
///
/// @param id      the module's stable identifier, e.g. `virtual-topics`
/// @param enabled whether the module is switched on
@JsonInclude(JsonInclude.Include.NON_NULL)
record ModuleConfigView(
        String id,
        boolean enabled,
        @Nullable String connectUrl
) {
}
