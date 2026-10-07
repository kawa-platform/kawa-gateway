package io.jonasg.kawa.http;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/// Reads `/admin/users` request bodies. Unknown fields are rejected rather than ignored, so a typo
/// such as `{"enable": false}` or a field the gateway owns such as `createdAt` fails loudly instead
/// of silently changing nothing.
final class AdminUserRequests {

    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private AdminUserRequests() {
    }

    /// Parses `body` as `type`.
    ///
    /// @throws IllegalArgumentException when the body is not valid JSON for `type`
    static <T> T read(byte[] body, Class<T> type) {
        try {
            return JSON.readValue(body, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid admin user body: " + e.getMessage(), e);
        }
    }
}
