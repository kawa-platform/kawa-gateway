package io.jonasg.kawa.http;

/// A module in an admin `/modules` response.
///
/// @param id      the module's stable identifier, e.g. `virtual-topics`
/// @param enabled whether the module is switched on
public record ModuleConfigView(String id, boolean enabled) {
}
