package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;

import java.util.ArrayList;

public final class MetadataVirtualTopicTransform
        implements VirtualTopicTransform<MetadataRequestData, MetadataResponseData> {

    private final VirtualTopicManager virtualTopics;
    private final AdvertisedListener advertised;

    public MetadataVirtualTopicTransform(
            VirtualTopicManager virtualTopics,
            AdvertisedListener advertised
    ) {
        this.virtualTopics = virtualTopics;
        this.advertised = advertised;
    }

    @Override
    public short apiKey() {
        return 3;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            MetadataRequestData data
    ) {
        if (data.topics() == null) {
            return;
        }
        for (MetadataRequestData.MetadataRequestTopic topic : data.topics()) {
            topic.setName(virtualTopics.toPhysical(topic.name()));
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            MetadataResponseData data
    ) {
        // Physical topics backing a virtual topic are hidden by default: renamed to their
        // virtual name in place rather than exposed alongside their physical name. A virtual
        // topic configured with exposePhysicalTopic: true opts back into the old behaviour and
        // gets both names listed - iterate a copy since that path adds to data.topics().
        for (MetadataResponseData.MetadataResponseTopic topic : new ArrayList<>(data.topics())) {
            if (!virtualTopics.hasVirtualTopic(topic.name())) {
                continue;
            }
            String virtual = virtualTopics.toVirtual(topic.name());
            if (virtualTopics.exposesPhysicalTopic(topic.name())) {
                data.topics().add(new MetadataResponseData.MetadataResponseTopic()
                        .setName(virtual)
                        .setTopicId(topic.topicId())
                        .setIsInternal(topic.isInternal())
                        .setPartitions(topic.partitions()));
            } else {
                topic.setName(virtual);
            }
        }

        for (MetadataResponseData.MetadataResponseBroker broker : data.brokers()) {
            broker.setHost(advertised.host());
            broker.setPort(advertised.port());
        }
    }
}
