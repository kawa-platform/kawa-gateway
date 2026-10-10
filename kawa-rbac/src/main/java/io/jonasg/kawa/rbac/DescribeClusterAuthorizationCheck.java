package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

/// Gates DescribeCluster the way Kafka does: any authenticated principal may describe the cluster,
/// so there is no ACL to check. A request without an authenticated principal is denied with
/// SASL_AUTHENTICATION_FAILED rather than passed through, so skipping SASL cannot bypass RBAC.
public final class DescribeClusterAuthorizationCheck
        implements AuthorizationCheck<DescribeClusterRequestData, DescribeClusterResponseData> {

    @Override
    public short apiKey() {
        return ApiKeys.DESCRIBE_CLUSTER.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, DescribeClusterRequestData body) {
        if (context.principal() == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new DescribeClusterResponseData()
                            .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                            .setErrorMessage(Errors.SASL_AUTHENTICATION_FAILED.message())));
        }
    }
}
