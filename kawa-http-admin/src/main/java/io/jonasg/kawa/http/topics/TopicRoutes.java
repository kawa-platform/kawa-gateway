package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.http.configchange.DeleteManagedByHandler;
import io.jonasg.kawa.http.web.Router;

/// Registers the `/topics` routes, backed by the [TopicService], on a [Router].
public final class TopicRoutes {

    private TopicRoutes() {
    }

    public static void register(Router router, TopicService topicService) {
        router.get("/topics", new GetTopicsHandler(topicService))
                .post("/topics", new PostTopicHandler(topicService))
                .put("/topics/{name}", new PutTopicHandler(topicService))
                .patch("/topics/{name}", new PatchTopicHandler(topicService))
                .delete("/topics/{name}", new DeleteTopicHandler(topicService))
                .delete("/topics/{name}/managed-by", new DeleteManagedByHandler(topicService::releaseVirtualTopic));
    }
}
