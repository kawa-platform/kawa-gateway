package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

/// A rename request for a single RBAC group: the new name the group is re-keyed under.
/// The group's clients and roles move with it.
///
/// @param name the new group name
@NullUnmarked
public record GroupConfigPatch(String name) {
}
