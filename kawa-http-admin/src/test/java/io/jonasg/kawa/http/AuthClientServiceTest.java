package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuthClientServiceTest {

    private final FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    private final AuthClientService service = new AuthClientService(repository);

    @Test
    void listsClients() {
        // given
        repository.update(base -> base.updateAuth(base.auth().upsertClient(
                "alice", ClientConfig.fromPlaintext("PLAIN", "secret"))));

        // when
        var result = service.listClients();

        // then
        assertThat(result).containsKey("alice");
    }

    @Test
    void putsClientAndSynchronizesGroups() {
        // given
        repository.update(base -> base.updateRbac(
                base.rbac().upsertGroup("producers", new GroupConfig(List.of(), List.of()))));
        var request = new ClientConfigRequest("PLAIN", "secret", List.of("producers"));

        // when
        var result = service.upsertClient("alice", request, Consistency.PERSISTED);

        // then
        assertThat(result.mechanism()).isEqualTo("PLAIN");
        assertThat(repository.getActiveConfig().auth().clients()).containsKey("alice");
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients()).containsExactly("alice");
    }

    @Test
    void patchesClient() {
        // given
        repository.update(base -> base.updateAuth(
                base.auth().upsertClient("alice", ClientConfig.fromPlaintext("PLAIN", "secret"))));
        var patch = new ClientConfigPatch(null, "new-secret", null);

        // when
        var result = service.updateClient("alice", patch, Consistency.PERSISTED);

        // then
        assertThat(result.mechanism()).isEqualTo("PLAIN");
        assertThat(repository.getActiveConfig().auth().clients().get("alice").password().verify("new-secret"))
                .isTrue();
    }

    @Test
    void deletesClientAndRemovesFromGroups() {
        // given
        repository.update(base -> base
                .updateAuth(base.auth().upsertClient("alice", ClientConfig.fromPlaintext("PLAIN", "secret")))
                .updateRbac(base.rbac().upsertGroup("producers", new GroupConfig(List.of("alice"), List.of()))));

        // when
        service.deleteClient("alice", Consistency.PERSISTED);

        // then
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients()).isEmpty();
    }
}
