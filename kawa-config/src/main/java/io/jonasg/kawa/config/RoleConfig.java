package io.jonasg.kawa.config;

import java.util.List;

/// A named role: the list of ACLs it grants or denies.
///
/// @param acls      the ACLs making up this role
/// @param managedBy the owner of this entry, or `null` when it is unmanaged
public record RoleConfig(List<AclConfig> acls, ManagedBy managedBy) {

    public RoleConfig {
        acls = acls == null ? List.of() : List.copyOf(acls);
    }

    public RoleConfig(List<AclConfig> acls) {
        this(acls, null);
    }

    /// Returns a copy owned by `managedBy` (`null` for unmanaged).
    public RoleConfig withManagedBy(ManagedBy managedBy) {
        return new RoleConfig(acls, managedBy);
    }
}
