package io.jonasg.kawa.http.topics;

public class TopicNotFoundException extends TopicServiceException {
    public TopicNotFoundException(String msg) {
        super(msg);
    }
}
