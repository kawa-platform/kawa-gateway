package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/// Serves `GET /topics`.
public final class GetTopicsHandler implements Router.Handler {

    private final TopicService topicService;

    public GetTopicsHandler(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public Router.Response<List<TopicView>> handle(Router.Request request) {
        var listing = topicService.listTopics();
        List<TopicView> views = new ArrayList<>();
        for (var entry : listing.virtual()) {
            views.add(new TopicView(
                    "virtual",
                    entry.name(),
                    topicService.partitionCount(entry.physicalTopic()),
                    topicService.replicationFactor(entry.physicalTopic()),
                    toFilterView(entry.filter()),
                    entry.physicalTopic(),
                    entry.exposePhysicalTopic(),
                    entry.valueFormat()));
        }
        for (TopicMetadata tm : listing.physical()) {
            views.add(new TopicView(
                    "physical",
                    tm.name(),
                    topicService.partitionCount(tm.name()),
                    topicService.replicationFactor(tm.name()),
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
