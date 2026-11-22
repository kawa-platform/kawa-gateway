package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.configchange.NotFoundException;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerFeatures;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.spec.McpSchema.TextResourceContents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/// An MCP resource: read-only data an agent attaches by URI, answered as the JSON of a view. A subclass
/// supplies its metadata through the constructor and its work through [#read(String)]; the base registers it
/// as a fixed resource ([#specification()]) or as a URI template ([#templateSpecification()]), guards every read
/// and logs it once.
///
/// `read` throws [NotFoundException] for a resource that does not exist, which the agent sees as the JSON-RPC
/// resource-not-found error, and [IllegalArgumentException] for a URI it cannot use, which becomes invalid params.
/// Any other failure is logged and answered with [McpTool#INTERNAL_ERROR], so internals do not leak to the agent.
///
/// Every read leaves one info line naming the resource, the caller and the OAuth client, e.g.
/// `MCP resource kafka://topics read by alice via kawa-mcp-claude: ok`.
public abstract class McpResource {

    private static final String JSON = "application/json";
    /// The most completion values one answer carries; the protocol caps a completion at 100.
    private static final int MAX_COMPLETIONS = 100;
    private static final Logger log = LoggerFactory.getLogger(McpResource.class);

    private final McpJsonMapper mapper;
    private final String uri;
    private final String name;
    private final String description;

    /// @param mapper      writes the resource's JSON; the server passes the mapper it serves with
    /// @param uri         the fixed URI, or a URI template with one trailing variable, e.g. `kafka://topics/{topicName}`
    /// @param name        the resource name the agent sees
    /// @param description what the resource holds, as the agent reads it
    protected McpResource(McpJsonMapper mapper, String uri, String name, String description) {
        this.mapper = mapper;
        this.uri = uri;
        this.name = name;
        this.description = description;
    }

    /// Reads the resource at `requestedUri`, which is the fixed URI itself or an instance of its template, and
    /// returns the view it answers with.
    protected abstract Object read(String requestedUri);

    /// The registration of a resource with a fixed URI.
    public final McpStatelessServerFeatures.SyncResourceSpecification specification() {
        var resource = McpSchema.Resource.builder(uri, name).description(description).mimeType(JSON).build();
        return new McpStatelessServerFeatures.SyncResourceSpecification(resource, this::guarded);
    }

    /// The registration of a resource whose URI is a template.
    public final McpStatelessServerFeatures.SyncResourceTemplateSpecification templateSpecification() {
        var template = McpSchema.ResourceTemplate.builder(uri, name).description(description).mimeType(JSON).build();
        return new McpStatelessServerFeatures.SyncResourceTemplateSpecification(template, this::guarded);
    }

    /// The values the template's variable may take that start with `prefix`, offered to the agent as autocomplete.
    /// None by default.
    protected List<String> completions(String prefix) {
        return List.of();
    }

    /// The autocomplete registration of a resource whose URI is a template: `completion/complete` for its variable
    /// is answered with [#completions(String)], capped at the [#MAX_COMPLETIONS] values the protocol allows.
    public final McpStatelessServerFeatures.SyncCompletionSpecification completionSpecification() {
        var reference = new McpSchema.ResourceReference(uri);
        var variable = uri.substring(uri.indexOf('{') + 1, uri.indexOf('}'));
        return new McpStatelessServerFeatures.SyncCompletionSpecification(reference, (context, request) -> {
            if (!variable.equals(request.argument().name())) {
                return new McpSchema.CompleteResult(new McpSchema.CompleteResult.CompleteCompletion(List.of()));
            }
            var values = completions(request.argument().value());
            var shown = values.stream().limit(MAX_COMPLETIONS).toList();
            return new McpSchema.CompleteResult(
                    new McpSchema.CompleteResult.CompleteCompletion(shown, values.size(), values.size() > MAX_COMPLETIONS));
        });
    }

    /// The value of the template's variable in `requestedUri`: the one path segment after the template's fixed
    /// prefix, percent-decoded.
    ///
    /// @throws IllegalArgumentException when `requestedUri` is not an instance of the template, the segment is
    ///                                  empty or it is not validly encoded
    protected final String variable(String requestedUri) {
        var prefix = uri.substring(0, uri.indexOf('{'));
        var segment = requestedUri.startsWith(prefix) ? requestedUri.substring(prefix.length()) : "";
        if (segment.isEmpty() || segment.contains("/")) {
            throw new IllegalArgumentException("uri must be " + uri);
        }
        // a name may contain a literal '+', which URLDecoder would turn into a space
        return URLDecoder.decode(segment.replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    private ReadResourceResult guarded(McpTransportContext context, ReadResourceRequest request) {
        var requested = request.uri();
        ReadResourceResult result = null;
        McpError failure = null;
        String outcome = "ok";
        try {
            result = ReadResourceResult.builder(List.of(new TextResourceContents(requested, JSON, json(read(requested)), null))).build();
        } catch (NotFoundException e) {
            failure = McpError.builder(McpSchema.ErrorCodes.RESOURCE_NOT_FOUND).message(e.getMessage())
                    .data(Map.of("uri", requested)).build();
            outcome = "error";
        } catch (IllegalArgumentException e) {
            failure = McpError.builder(McpSchema.ErrorCodes.INVALID_PARAMS).message(e.getMessage()).build();
            outcome = "error";
        } catch (RuntimeException e) {
            log.warn("MCP resource {} failed", requested, e);
            failure = McpError.builder(McpSchema.ErrorCodes.INTERNAL_ERROR).message(McpTool.INTERNAL_ERROR).build();
            outcome = "failed";
        }
        log.info("MCP resource {} read by {}: {}", requested, McpCaller.describe(context), outcome);
        if (failure != null) {
            throw failure;
        }
        return result;
    }

    private String json(Object view) {
        try {
            return mapper.writeValueAsString(view);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
