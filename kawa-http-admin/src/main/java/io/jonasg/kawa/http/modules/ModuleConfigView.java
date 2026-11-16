package io.jonasg.kawa.http.modules;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

/// A module in an admin `/modules` response.
///
/// @param id                the module's stable identifier, e.g. `virtual-topics`
/// @param enabled           whether the module is switched on
/// @param available         whether the module can run on this gateway, whether or not it is switched on;
///                          an unavailable module cannot be switched on
/// @param unavailableReason why the module is not available, e.g. that `admin.publicUrl` is not set;
///                          `null` and left out of the JSON while it is available
/// @param connectUrl        the URL agents connect to, `<admin.publicUrl>/mcp`: only on the `mcp` module while
///                          it is on and available, otherwise `null` and left out of the JSON
@JsonInclude(JsonInclude.Include.NON_NULL)
record ModuleConfigView(
        String id,
        boolean enabled,
        boolean available,
        @Nullable String unavailableReason,
        @Nullable String connectUrl
) {
}
