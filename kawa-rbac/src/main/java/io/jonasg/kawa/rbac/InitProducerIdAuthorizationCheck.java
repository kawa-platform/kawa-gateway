package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

/// Gates InitProducerId for transactional producers on TRANSACTIONAL_ID WRITE for the
/// requested transactional id, as Kafka does. An idempotent producer (no transactional id) is
/// forwarded unchanged: what it writes is gated per topic on Produce.
public final class InitProducerIdAuthorizationCheck
        implements AuthorizationCheck<InitProducerIdRequestData, InitProducerIdResponseData> {

    private final RbacAuthorizer authorizer;

    public InitProducerIdAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.INIT_PRODUCER_ID.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, InitProducerIdRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, denial(Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, denial(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)));
            return;
        }
        if (data.transactionalId() == null) {
            return;
        }
        if (!authorizer.isAuthorized(principal, ResourceType.TRANSACTIONAL_ID, data.transactionalId(), AclOperation.WRITE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, denial(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)));
        }
    }

    private static InitProducerIdResponseData denial(Errors error) {
        return new InitProducerIdResponseData()
                .setErrorCode(error.code())
                .setProducerId(-1L)
                .setProducerEpoch((short) -1);
    }
}
