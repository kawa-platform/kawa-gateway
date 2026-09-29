package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GovernanceServiceTest {

    private final FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    private final GovernanceService service = new GovernanceService(repository);

    @Test
    void returnsCurrentGovernanceConfig() {
        // given
        var config = new GovernanceConfig(
                Map.of("min-replication", new GovernanceRuleConfig("msg", "true")),
                Map.of());
        repository.update(base -> base.updateGovernance(config));

        // when
        GovernanceConfig result = service.get();

        // then
        assertThat(result.topicRules()).containsKey("min-replication");
    }

    @Test
    void persistsGovernanceConfig() {
        // given
        var request = new GovernanceConfigRequest(
                Map.of("min-replication", new GovernanceRuleRequest("msg", "true")),
                Map.of());

        // when
        GovernanceConfig result = service.updateGovernance(request, Consistency.PERSISTED);

        // then
        assertThat(result.topicRules()).containsKey("min-replication");
        assertThat(repository.getActiveConfig().governance().topicRules()).containsKey("min-replication");
    }
}
