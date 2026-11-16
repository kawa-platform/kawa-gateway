package io.jonasg.kawa.http.modules;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

/// A module in an admin `/modules` response.
///
/// @param id         the module's stable identifier, e.g. `virtual-topics`
/// @param enabled    whether the module is switched on
/// @param connectUrl the URL agents connect to, `<admin.publicUrl>/mcp`: only on the `mcp` module while it is on
///                   and `admin.publicUrl` is set, otherwise `null` and left out of the JSON
@JsonInclude(JsonInclude.Include.NON_NULL)
record ModuleConfigView(
        String id,
        boolean enabled,
        @Nullable String connectUrl
) {
}
