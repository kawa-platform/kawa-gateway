package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Response;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerEndpointInterceptorTest {

    private final BrokerEndpointInterceptor interceptor =
            new BrokerEndpointInterceptor(AdvertisedListener.of(1, "gateway.example.com", 9092));

    private final GatewayContext context = new GatewayContext("test", 0L);

    @Test
    void pointsDescribedBrokersAtTheAdvertisedListener() {
        // given
        var data = new DescribeClusterResponseData();
        data.brokers().add(new DescribeClusterResponseData.DescribeClusterBroker()
                .setBrokerId(0).setHost("broker-internal").setPort(9093));
        data.brokers().add(new DescribeClusterResponseData.DescribeClusterBroker()
                .setBrokerId(1).setHost("broker-internal-2").setPort(9093));
        var response = response(KafkaApiRegistry.DESCRIBE_CLUSTER, data);

        // when
        interceptor.onResponse(context, response);

        // then
        assertThat(interceptor.appliesToResponse(response))
                .withFailMessage(() -> "DescribeCluster responses were not picked up by the interceptor")
                .isTrue();
        assertThat(data.brokers())
                .withFailMessage(() -> "Not every described broker points at the gateway: " + data.brokers())
                .allSatisfy(broker -> {
                    assertThat(broker.host()).isEqualTo("gateway.example.com");
                    assertThat(broker.port()).isEqualTo(9092);
                });
    }

    @Test
    void leavesControllerEndpointsUntouched() {
        // given
        var data = new DescribeClusterResponseData().setEndpointType((byte) 2);
        data.brokers().add(new DescribeClusterResponseData.DescribeClusterBroker()
                .setBrokerId(3000).setHost("controller-internal").setPort(9094));

        // when
        interceptor.onResponse(context, response(KafkaApiRegistry.DESCRIBE_CLUSTER, data));

        // then
        assertThat(data.brokers().iterator().next().host())
                .withFailMessage(() -> "A controller endpoint was rewritten to the gateway: " + data.brokers())
                .isEqualTo("controller-internal");
    }

    @Test
    void ignoresRequestsOtherApisAndPassThroughBodies() {
        // given
        var produce = response(KafkaApiRegistry.PRODUCE, new Object());
        var passThrough = response(KafkaApiRegistry.DESCRIBE_CLUSTER, null);

        // when / then
        assertThat(interceptor.appliesToResponse(produce))
                .withFailMessage(() -> "Produce responses were picked up by the interceptor")
                .isFalse();
        interceptor.onResponse(context, passThrough); // no decoded body: left alone, not cast
        assertThat(interceptor.appliesToRequest(null))
                .withFailMessage(() -> "The interceptor applied to a request; it only rewrites responses")
                .isFalse();
    }

    private static Response response(
            int apiKey,
            Object body
    ) {
        return new Response() {
            @Override
            public int apiKey() {
                return apiKey;
            }

            @Override
            public String apiName() {
                return "test";
            }

            @Override
            public short apiVersion() {
                return 1;
            }

            @Override
            public int correlationId() {
                return 1;
            }

            @Override
            public Object body() {
                return body;
            }
        };
    }
}
