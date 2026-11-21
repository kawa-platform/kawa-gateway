package io.jonasg.kawa.mcp.listconsumergroups;

import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.mcp.McpTool;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;

/// The `list_consumer_groups` tool: every group on the cluster with its type, state and, where it applies, lag, read
/// from the [ConsumerGroupService] and answered as a [ListConsumerGroupsView], both as structured content
/// and as its JSON text. It takes no arguments.
public final class ListConsumerGroupsMcpTool extends McpTool {

    private static final String NAME = "list_consumer_groups";

    private static final String INPUT_SCHEMA = """
            {
              "type": "object",
              "properties": {}
            }
            """;

    private final ConsumerGroupService consumerGroupService;

    public ListConsumerGroupsMcpTool(McpJsonMapper mapper, ConsumerGroupService consumerGroupService) {
        super(mapper);
        this.consumerGroupService = consumerGroupService;
    }

    @Override
    protected String name() {
        return NAME;
    }

    @Override
    protected String description() {
        return "Lists every group on the cluster with its type, state, total lag and the number of partitions "
                + "it reads without a commit, to spot a stuck or lagging group. Lag is measured against the physical "
                + "topic; when `lagIsUpperBound` is true, the group reads a filtered virtual topic and records the "
                + "filter drops still count as lag. Only `CONSUMER` groups and `CLASSIC` groups of protocol type `consumer` "
                + "have lag; share, streams, Connect and other groups are listed with `lagApplicable: false` and no lag.";
    }

    @Override
    protected String inputSchema() {
        return INPUT_SCHEMA;
    }

    @Override
    protected String outputSchema() {
        return ListConsumerGroupsView.SCHEMA;
    }

    @Override
    protected ListConsumerGroupsView call(CallToolRequest request) {
        return ListConsumerGroupsView.of(consumerGroupService.list());
    }
}
