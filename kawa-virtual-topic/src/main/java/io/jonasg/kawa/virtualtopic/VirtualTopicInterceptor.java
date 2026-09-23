package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.virtualtopic.filter.VirtualTopicRecordFilter;
import io.jonasg.kawa.virtualtopic.transform.AddPartitionsToTxnVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.AlterConfigsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.CreateAclsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.CreateTopicVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.CreatePartitionsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DeleteAclsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DeleteRecordsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DeleteTopicVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DescribeAclsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DescribeConfigsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DescribeLogDirsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DescribeTopicPartitionsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.DescribeTransactionsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.FetchVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.FindCoordinatorVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.IncrementalAlterConfigsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.ListOffsetsVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.MetadataVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.OffsetCommitVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.OffsetDeleteVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.OffsetFetchVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.OffsetForLeaderEpochVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.ProduceVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.TxnOffsetCommitVirtualTopicTransform;
import io.jonasg.kawa.virtualtopic.transform.VirtualTopicTransformRegistry;

import java.util.List;

/// On the client-to-broker path it rewrites virtual
/// topic names to their physical topics; on the broker-to-client path it maps them back and
/// points every broker/coordinator endpoint at the gateway's advertised listener.
///
/// Only the APIs registered in the gateway's decode set are inspected; all other
/// requests pass through untouched.
public final class VirtualTopicInterceptor implements Interceptor {

    private final VirtualTopicTransformRegistry registry;

    public VirtualTopicInterceptor(
            VirtualTopicManager virtualTopics,
            AdvertisedListener advertised
    ) {
        this(virtualTopics, advertised, new FetchSessionRegistry());
    }

    public VirtualTopicInterceptor(
            VirtualTopicManager virtualTopics,
            AdvertisedListener advertised,
            FetchSessionRegistry fetchSessions
    ) {
        this.registry = defaultRegistry(
                virtualTopics, advertised, fetchSessions,
                new VirtualTopicRecordFilter());
    }

    private static VirtualTopicTransformRegistry defaultRegistry(
            VirtualTopicManager virtualTopics,
            AdvertisedListener advertised,
            FetchSessionRegistry fetchSessions,
            VirtualTopicRecordFilter recordFilter) {
        return new VirtualTopicTransformRegistry(List.of(
                new MetadataVirtualTopicTransform(virtualTopics, advertised),
                new ProduceVirtualTopicTransform(virtualTopics),
                new FetchVirtualTopicTransform(virtualTopics, fetchSessions, recordFilter),
                new ListOffsetsVirtualTopicTransform(virtualTopics),
                new OffsetCommitVirtualTopicTransform(virtualTopics),
                new OffsetFetchVirtualTopicTransform(virtualTopics),
                new FindCoordinatorVirtualTopicTransform(advertised),
                new CreateTopicVirtualTopicTransform(virtualTopics),
                new CreatePartitionsVirtualTopicTransform(virtualTopics),
                new DeleteTopicVirtualTopicTransform(virtualTopics),
                new DescribeTopicPartitionsVirtualTopicTransform(virtualTopics),
                new DescribeLogDirsVirtualTopicTransform(virtualTopics),
                new OffsetForLeaderEpochVirtualTopicTransform(virtualTopics),
                new TxnOffsetCommitVirtualTopicTransform(virtualTopics),
                new DescribeConfigsVirtualTopicTransform(virtualTopics),
                new CreateAclsVirtualTopicTransform(virtualTopics),
                new DeleteAclsVirtualTopicTransform(virtualTopics),
                new DescribeAclsVirtualTopicTransform(virtualTopics),
                new DescribeTransactionsVirtualTopicTransform(virtualTopics),
                new AddPartitionsToTxnVirtualTopicTransform(virtualTopics),
                new OffsetDeleteVirtualTopicTransform(virtualTopics),
                new AlterConfigsVirtualTopicTransform(virtualTopics),
                new IncrementalAlterConfigsVirtualTopicTransform(virtualTopics),
                new DeleteRecordsVirtualTopicTransform(virtualTopics)));
    }

    @Override
    public boolean appliesToRequest(Request request) {
        return registry.hasApiKey((short) request.apiKey());
    }

    @Override
    public boolean appliesToResponse(Response response) {
        return registry.hasApiKey((short) response.apiKey());
    }

    @Override
    public void onRequest(
            GatewayContext context,
            Request request
    ) {
        VirtualTopicState.from(context).apiVersion(request.apiVersion());
        registry.onRequest(context, (short) request.apiKey(), request.body());
    }

    @Override
    public void onResponse(
            GatewayContext context,
            Response response
    ) {
        registry.onResponse(context, (short) response.apiKey(), response.body());
    }
}
