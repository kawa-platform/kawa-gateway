package io.jonasg.kawa.mcp.listconsumergroups;

import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.mcp.McpResource;
import io.modelcontextprotocol.json.McpJsonMapper;

/// The `kawa://consumer-groups` resource: every group on the cluster, as the [ListConsumerGroupsView] the
/// `list_consumer_groups` tool answers with.
public final class ConsumerGroupsMcpResource extends McpResource {

    public static final String URI = "kawa://consumer-groups";

    private final ConsumerGroupService consumerGroupService;

    public ConsumerGroupsMcpResource(McpJsonMapper mapper, ConsumerGroupService consumerGroupService) {
        super(mapper, URI, "consumer-groups",
                "Every group on the cluster with its type, state, total lag and the number of partitions it reads "
                        + "without a commit. Lag applies only to consumer groups; other groups are listed without it.");
        this.consumerGroupService = consumerGroupService;
    }

    @Override
    protected ListConsumerGroupsView read(String requestedUri) {
        return ListConsumerGroupsView.of(consumerGroupService.list());
    }
}
