package io.jonasg.kawa.http.rbac;

import io.jonasg.kawa.config.AclConfig;

import java.util.List;

/// A role upsert request body.
///
/// @param acls the ACLs making up this role
record RoleConfigRequest(List<AclConfig> acls) {

    public RoleConfigRequest {
        acls = acls == null ? List.of() : List.copyOf(acls);
    }
}
