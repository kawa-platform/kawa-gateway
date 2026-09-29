package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RbacGroupServiceTest {

    private final FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    private final RbacGroupService service = new RbacGroupService(repository);

    @Test
    void listsGroups() {
        // given
        repository.update(base -> base.updateRbac(base.rbac().upsertGroup("producers", new GroupConfig(List.of(), List.of()))));

        // when
        var result = service.listGroups();

        // then
        assertThat(result).containsKey("producers");
    }

    @Test
    void putsGroup() {
        // given
        var request = new GroupConfigRequest(List.of("alice"), List.of("reader"));

        // when
        GroupConfig result = service.upsertGroup("producers", request, Consistency.PERSISTED);

        // then
        assertThat(result.clients()).containsExactly("alice");
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
    }

    @Test
    void patchesGroupRename() {
        // given
        repository.update(base -> base.updateRbac(
                base.rbac().upsertGroup("producers", new GroupConfig(List.of("alice"), List.of()))));
        var patch = new GroupConfigPatch("consumers");

        // when
        GroupConfig result = service.updateGroup("producers", patch, Consistency.PERSISTED);

        // then
        assertThat(result.clients()).containsExactly("alice");
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("consumers");
        assertThat(repository.getActiveConfig().rbac().groups()).doesNotContainKey("producers");
    }

    @Test
    void throwsConflictExceptionWhenGroupHasClientsOnDelete() {
        // given
        repository.update(base -> base.updateRbac(
                base.rbac().upsertGroup("producers", new GroupConfig(List.of("alice"), List.of()))));

        // then
        assertThatThrownBy(() -> service.deleteGroup("producers", Consistency.PERSISTED))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining("still has clients");
    }

    @Test
    void deletesGroup() {
        // given
        repository.update(base -> base.updateRbac(
                base.rbac().upsertGroup("producers", new GroupConfig(List.of(), List.of()))));

        // when
        service.deleteGroup("producers", Consistency.PERSISTED);

        // then
        assertThat(repository.getActiveConfig().rbac().groups()).isEmpty();
    }
}
