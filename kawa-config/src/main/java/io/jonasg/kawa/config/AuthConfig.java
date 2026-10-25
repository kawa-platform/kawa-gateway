package io.jonasg.kawa.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record AuthConfig(
        Set<String> mechanisms,
        Map<String, ClientConfig> clients,
        BrokerAuthConfig brokerAuth
) {

    public AuthConfig {
        mechanisms = mechanisms == null ? Set.of() : Set.copyOf(mechanisms);
        clients = clients == null ? Map.of() : resolveClientMechanisms(clients, mechanisms);
        validateClientMechanisms(clients, mechanisms);
    }

    /// Returns a new [AuthConfig] with the given client added or replaced. The client's
    /// mechanism is required. It is ensured present in the advertised [mechanisms] list
    /// (added when missing), so the first client can be added to an empty config via the
    /// admin API. The startup-only [brokerAuth] is preserved.
    public AuthConfig upsertClient(String username, ClientConfig client) {
        if (client == null) {
            throw new IllegalArgumentException("client config for '" + username + "' must not be null");
        }
        String mechanism = client.mechanism();
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("client '" + username + "' must specify a mechanism");
        }
        var newClients = new HashMap<>(clients);
        newClients.put(username, client);
        Set<String> newMechanisms = mechanisms;
        if (!mechanisms.contains(mechanism)) {
            var expanded = new HashSet<>(mechanisms);
            expanded.add(mechanism);
            newMechanisms = Set.copyOf(expanded);
        }
        return new AuthConfig(newMechanisms, newClients, brokerAuth);
    }

    /// Returns a new [AuthConfig] with the given client removed. The startup-only
    /// [brokerAuth] is preserved.
    public AuthConfig removeClient(String username) {
        var newClients = new HashMap<>(clients);
        newClients.remove(username);
        return new AuthConfig(mechanisms, newClients, brokerAuth);
    }

    private static Map<String, ClientConfig> resolveClientMechanisms(
            Map<String, ClientConfig> clients,
            Set<String> mechanisms
    ) {
        // Deterministic default: pick the alphabetically-first configured mechanism so a
        // client without an explicit mechanism resolves consistently regardless of Set order.
        String globalMechanism = mechanisms.stream().sorted().findFirst().orElse(null);
        return clients.entrySet().stream().collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> resolveClient(entry.getKey(), entry.getValue(), globalMechanism)));
    }

    private static ClientConfig resolveClient(
            String username,
            ClientConfig client,
            String globalMechanism
    ) {
        if (client == null) {
            throw new IllegalArgumentException("client config for '" + username + "' must not be null");
        }
        String mechanism = client.mechanism();
        if (mechanism == null || mechanism.isBlank()) {
            if (globalMechanism == null) {
                throw new IllegalArgumentException("client '" + username
                                                   + "' has no mechanism and no global mechanism is configured");
            }
            return new ClientConfig(globalMechanism, client.password(), client.managedBy());
        }
        return client;
    }

    private static void validateClientMechanisms(
            Map<String, ClientConfig> clients,
            Set<String> mechanisms
    ) {
        for (var entry : clients.entrySet()) {
            String username = entry.getKey();
            String mechanism = entry.getValue().mechanism();
            if (!mechanisms.contains(mechanism)) {
                throw new IllegalArgumentException(
                        "Client '" + username + "' uses mechanism '" + mechanism
                        + "' which is not in the configured mechanisms list " + mechanisms);
            }
        }
    }
}
