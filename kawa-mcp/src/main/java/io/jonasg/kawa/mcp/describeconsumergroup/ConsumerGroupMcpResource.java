package io.jonasg.kawa.mcp.describeconsumergroup;

import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.mcp.McpResource;
import io.modelcontextprotocol.json.McpJsonMapper;

/// The `kawa://consumer-groups/{groupId}` resource template: one group, as the [ConsumerGroupDescriptionView] the
/// `describe_consumer_group` tool answers with. An unknown group is a resource-not-found error.
public final class ConsumerGroupMcpResource extends McpResource {

    public static final String URI_TEMPLATE = "kawa://consumer-groups/{groupId}";

    private final ConsumerGroupService consumerGroupService;

    public ConsumerGroupMcpResource(McpJsonMapper mapper, ConsumerGroupService consumerGroupService) {
        super(mapper, URI_TEMPLATE, "consumer-group",
                "One group by its id: its members with the partitions each holds, and the committed offset, end "
                        + "offset and lag of every partition it reads. Lag applies only to consumer groups.");
        this.consumerGroupService = consumerGroupService;
    }

    @Override
    protected ConsumerGroupDescriptionView read(String requestedUri) {
        return ConsumerGroupDescriptionView.of(consumerGroupService.describe(variable(requestedUri)));
    }
}
