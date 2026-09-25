package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Projects the virtual and physical topics served by `GET /topics` from the [VirtualTopicManager]
/// (virtual config) and the [MetadataCache] (live physical topology). Plain handler with no Netty
/// imports; the [HttpRouterHandler] dispatcher serializes the result and writes the response.
public final class GetTopicsHandler implements Router.Handler {

    private final VirtualTopicManager virtualTopics;
    private final MetadataCache cache;

    public GetTopicsHandler(VirtualTopicManager virtualTopics, MetadataCache cache) {
        this.virtualTopics = virtualTopics;
        this.cache = cache;
    }

    @Override
    public Router.Response<List<TopicView>> handle(Router.Request request) {
        List<TopicView> views = new ArrayList<>();
        for (Map.Entry<String, String> entry : virtualTopics.virtualTopics().entrySet()) {
            String virtual = entry.getKey();
            String physical = entry.getValue();
            views.add(new TopicView(
                    "virtual",
                    virtual,
                    cache.partitionCount(physical),
                    cache.replicationFactor(physical),
                    toFilterView(virtualTopics.filterFor(virtual).orElse(null)),
                    physical,
                    virtualTopics.exposesPhysicalTopic(virtual),
                    virtualTopics.valueFormatFor(virtual).orElse(null)));
        }
        for (TopicMetadata tm : cache.topics()) {
            views.add(new TopicView(
                    "physical",
                    tm.name(),
                    cache.partitionCount(tm.name()),
                    cache.replicationFactor(tm.name()),
                    null,
                    null,
                    null,
                    null));
        }
        return Router.Response.ok(views);
    }

    private static @Nullable TopicFilterView toFilterView(@Nullable VirtualTopicFilterConfig filter) {
        return switch (filter) {
            case null -> null;
            case HeaderEqualsFilterConfig cfg -> new TopicFilterView("header", cfg.header() + "=" + cfg.value());
            case HeaderContainsFilterConfig cfg -> new TopicFilterView("headerContains", cfg.header() + " contains " + cfg.value());
            case HeaderStartsWithFilterConfig cfg -> new TopicFilterView("headerStartsWith", cfg.header() + " starts with " + cfg.value());
            case HeaderMatchesFilterConfig cfg -> new TopicFilterView("headerMatches", cfg.header() + " matches " + cfg.value());
            case CelFilterConfig cfg -> new TopicFilterView("cel", cfg.expression());
        };
    }
}
