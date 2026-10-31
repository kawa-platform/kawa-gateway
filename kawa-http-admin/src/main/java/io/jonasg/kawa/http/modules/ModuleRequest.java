package io.jonasg.kawa.http.modules;

import org.jspecify.annotations.NullUnmarked;

/// A module in a `PUT /modules/{id}` body. Validated and converted by [ModuleConfigMapper].
///
/// @param enabled whether the module is switched on; required
@NullUnmarked
record ModuleRequest(Boolean enabled) {
}
