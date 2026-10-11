package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

/// Gates AddOffsetsToTxn as Kafka does: the producer needs TRANSACTIONAL_ID WRITE on its
/// transactional id and GROUP READ on the group whose offsets it adds to the transaction. A request
/// without an authenticated principal is denied with SASL_AUTHENTICATION_FAILED rather than passed
/// through, so skipping SASL cannot bypass RBAC.
public final class AddOffsetsToTxnAuthorizationCheck
        implements AuthorizationCheck<AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData> {

    private final RbacAuthorizer authorizer;

    public AddOffsetsToTxnAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.ADD_OFFSETS_TO_TXN.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, AddOffsetsToTxnRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, denial(Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null
                || !authorizer.isAuthorized(principal, ResourceType.TRANSACTIONAL_ID, data.transactionalId(), AclOperation.WRITE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, denial(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)));
            return;
        }
        if (!authorizer.isAuthorized(principal, ResourceType.GROUP, data.groupId(), AclOperation.READ)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, denial(Errors.GROUP_AUTHORIZATION_FAILED)));
        }
    }

    private static AddOffsetsToTxnResponseData denial(Errors error) {
        return new AddOffsetsToTxnResponseData().setErrorCode(error.code());
    }
}
