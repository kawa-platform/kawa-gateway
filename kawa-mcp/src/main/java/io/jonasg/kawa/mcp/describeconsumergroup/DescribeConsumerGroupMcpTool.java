package io.jonasg.kawa.mcp.describeconsumergroup;

import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.mcp.McpTool;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;

/// The `describe_consumer_group` tool: one group with its members and the lag of each partition it reads (or, when
/// lag does not apply, only its type and state), read from the [ConsumerGroupService] and answered as a
/// [ConsumerGroupDescriptionView]. The `groupId` argument names the group; an unknown group is a tool error.
public final class DescribeConsumerGroupMcpTool extends McpTool {

    private static final String NAME = "describe_consumer_group";
    private static final String INPUT_SCHEMA = """
            {
              "type": "object",
              "properties": {
                "groupId": {
                  "type": "string"
                }
              },
              "required": ["groupId"]
            }""";

    private final ConsumerGroupService consumerGroupService;

    public DescribeConsumerGroupMcpTool(McpJsonMapper mapper, ConsumerGroupService consumerGroupService) {
        super(mapper);
        this.consumerGroupService = consumerGroupService;
    }

    @Override
    protected String name() {
        return NAME;
    }

    @Override
    protected String description() {
        return "Describes one group by its id: its members with the partitions each holds, and the committed "
                + "offset, end offset and lag of every partition it reads, to find out why a group lags. Lag is measured "
                + "against the physical topic; when `lagIsUpperBound` is true, records a virtual topic's filter drops "
                + "still count as lag. Only `CONSUMER` groups and `CLASSIC` groups of protocol type `consumer` have lag; "
                + "share, streams, Connect and other groups are described by type and state with `lagApplicable: false`.";
    }

    @Override
    protected String inputSchema() {
        return INPUT_SCHEMA;
    }

    @Override
    protected String outputSchema() {
        return ConsumerGroupDescriptionView.SCHEMA;
    }

    @Override
    protected ConsumerGroupDescriptionView call(CallToolRequest request) {
        return ConsumerGroupDescriptionView.of(consumerGroupService.describe(getRequiredArg(request, "groupId")));
    }
}
