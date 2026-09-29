package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RbacRoleServiceTest {

    private final FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    private final RbacRoleService service = new RbacRoleService(repository);

    @Test
    void listsRoles() {
        // given
        repository.update(base -> base.updateRbac(base.rbac().upsertRole("reader", new RoleConfig(null))));

        // when
        var result = service.listRoles();

        // then
        assertThat(result).containsKey("reader");
    }

    @Test
    void putsRole() {
        // given
        var request = new RoleConfigRequest(List.of());

        // when
        RoleConfig result = service.upsertRole("reader", request, Consistency.PERSISTED);

        // then
        assertThat(result.acls()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().roles()).containsKey("reader");
    }

    @Test
    void deletesRoleAndRemovesFromGroups() {
        // given
        repository.update(base -> base.updateRbac(
                base.rbac()
                        .upsertRole("reader", new RoleConfig(null))
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));

        // when
        service.deleteRole("reader", Consistency.PERSISTED);

        // then
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").roles()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients()).containsExactly("alice");
    }
}
