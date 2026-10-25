package io.jonasg.kawa.http;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [GetModulesHandler], [GetModuleHandler] and [PutModuleHandler].
class ModuleSliceTest extends AdminHttpSliceTestBase {

    // --- GET /modules -------------------------------------------------------------------------

    @Test
    void listsEveryModuleAsEnabledWhenNothingIsStored() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/modules");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"id":"governance","enabled":true},
                  {"id":"rbac","enabled":true},
                  {"id":"virtual-topics","enabled":true}
                ]
                """);
    }

    @Test
    void listsEveryModuleWithItsStoredStateSortedById() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertModule(new ModuleConfig(ModuleId.GOVERNANCE, false)));
        startServer();

        // when
        var response = send("GET", "/modules");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"id":"governance","enabled":false},
                  {"id":"rbac","enabled":true},
                  {"id":"virtual-topics","enabled":true}
                ]
                """);
    }

    // --- GET /modules/{id} --------------------------------------------------------------------

    @Test
    void getsAStoredModule() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertModule(new ModuleConfig(ModuleId.RBAC, false)));
        startServer();

        // when
        var response = send("GET", "/modules/rbac");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"id":"rbac","enabled":false}
                """);
    }

    @Test
    void getsAModuleThatIsNotStoredAsEnabled() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/modules/rbac");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"id":"rbac","enabled":true}
                """);
    }

    @Test
    void getOfAnUnknownModuleIsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/modules/rbca");

        // then
        assertThat(response).hasStatusCode(404);
        assertThat(response.body()).contains("module 'rbca' not found");
    }

    // --- PUT /modules/{id} --------------------------------------------------------------------

    @Test
    void storesAModule() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/modules/virtual-topics", """
                {"enabled":false}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"id":"virtual-topics","enabled":false}
                """);
        assertThat(repository.getActiveConfigOrEmpty().modules().get("virtual-topics"))
                .withFailMessage(() -> "module 'virtual-topics' was not stored: " + repository.getActiveConfigOrEmpty().modules())
                .isEqualTo(new ModuleConfig(ModuleId.VIRTUAL_TOPICS, false));
    }

    @Test
    void replacesAStoredModule() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertModule(new ModuleConfig(ModuleId.RBAC, false)));
        startServer();

        // when
        var response = send("PUT", "/modules/rbac?consistency=applied", """
                {"enabled":true}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThat(repository.getActiveConfigOrEmpty().modules().get("rbac").enabled())
                .withFailMessage(() -> "module 'rbac' was not switched back on")
                .isTrue();
    }

    @Test
    void putOfAnUnknownModuleIsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/modules/rbca", """
                {"enabled":false}
                """);

        // then
        assertThat(response).hasStatusCode(404);
        assertThat(response.body()).contains("module 'rbca' not found");
        assertThat(repository.getActiveConfigOrEmpty().modules())
                .withFailMessage(() -> "an unknown module was stored: " + repository.getActiveConfigOrEmpty().modules())
                .isEmpty();
    }

    @Test
    void putWithoutEnabledIsABadRequest() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/modules/rbac", "{}");

        // then
        assertThat(response).hasStatusCode(400);
        assertThat(response.body()).contains("module 'rbac': enabled is required");
    }
}
