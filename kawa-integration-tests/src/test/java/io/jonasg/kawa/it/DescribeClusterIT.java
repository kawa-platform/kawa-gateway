package io.jonasg.kawa.it;

import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/// `AdminClient.describeCluster()` through the gateway, which sends DescribeCluster: every node it
/// reports must be the gateway's advertised listener, never a broker's own address, or the client
/// connects around the gateway.
class DescribeClusterIT extends GatewayTestSupport {

    @Test
    void describesTheClusterWithTheGatewayAsEveryNode() throws Exception {
        // when
        Collection<Node> nodes = gatewayAdmin.describeCluster().nodes().get(30, TimeUnit.SECONDS);

        // then
        assertThat(nodes)
                .withFailMessage(() -> "The cluster was described without any node")
                .isNotEmpty();
        assertThat(nodes)
                .withFailMessage(() -> "Not every node points at the gateway (localhost:" + gateway.boundPort() + "): " + nodes)
                .allSatisfy(node -> {
                    assertThat(node.host()).isEqualTo("localhost");
                    assertThat(node.port()).isEqualTo(gateway.boundPort());
                });
    }
}
