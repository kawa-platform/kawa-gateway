package io.jonasg.kawa.http.modules;

/// A module in an admin `/modules` response.
///
/// @param id      the module's stable identifier, e.g. `virtual-topics`
/// @param enabled whether the module is switched on
record ModuleConfigView(String id, boolean enabled) {
}
