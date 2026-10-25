package io.jonasg.kawa.config;

/// Marks a config entry as owned by an external manager (e.g. the kawa operator reconciling it from
/// Kubernetes). Only `principal` may change or release a managed entry; everyone else is refused.
/// An entry without a marker (`null`, also every entry of a snapshot written before markers existed)
/// is unmanaged.
///
/// @param principal the caller that owns the entry, e.g. `kawa-operator`
public record ManagedBy(String principal) {

    public ManagedBy {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("managedBy principal must not be null or blank");
        }
    }
}
