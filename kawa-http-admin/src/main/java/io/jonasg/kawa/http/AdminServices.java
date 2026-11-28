package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataRefresher;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.http.clients.AuthClientService;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.http.governance.GovernanceService;
import io.jonasg.kawa.http.modules.ModuleService;
import io.jonasg.kawa.http.oauthclients.OAuthClientService;
import io.jonasg.kawa.http.oauthclients.StoredOAuthClients;
import io.jonasg.kawa.http.rbac.RbacGroupService;
import io.jonasg.kawa.http.rbac.RbacRoleService;
import io.jonasg.kawa.broker.BrokerAdmin;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.http.users.AdminSetup;
import io.jonasg.kawa.http.users.AdminUserAuthenticator;
import io.jonasg.kawa.http.users.AdminUserService;
import io.jonasg.kawa.http.users.BootstrapUserSeeder;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import org.jspecify.annotations.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Function;

/// The services behind the admin surface, built once and shared by every surface that exposes
/// them (the [AdminHttpServer] routes). Closing them closes the [BrokerAdmin] they were built with,
/// so close them only after every surface has stopped.
///
/// @param adminUserAuthenticator checks admin user credentials for the authentication provider
/// @param oauthClientDirectory   the stored OAuth clients, for the authentication provider
/// @param setup                  setup mode: creating the first admin user of an `oauth` gateway without any
public record AdminServices(
        TopicService topics,
        ConsumerGroupService consumerGroups,
        RbacRoleService roles,
        RbacGroupService groups,
        AuthClientService clients,
        GovernanceService governanceRules,
        AdminUserService adminUsers,
        AdminUserAuthenticator adminUserAuthenticator,
        AdminSetup setup,
        ModuleService modules,
        OAuthClientService oauthClients,
        StoredOAuthClients oauthClientDirectory
) implements AutoCloseable {

    /// Builds the services, seeds `admin.auth.bootstrapUser` into the admin users (see
    /// [BootstrapUserSeeder]) and then decides on setup mode (see [AdminSetup]), so it must be called
    /// after the config topic has been read and before the admin authentication provider is created.
    ///
    /// @param metadataRefresher       refreshes `cache` on demand, so a created topic is listed at once
    /// @param topicVisibilityTimeout  how long a topic creation waits for the created topic to reach `cache`
    /// @param schemaRegistries        the schema registry of each physical topic's cluster, see [TopicService]
    /// @param auth                    `admin.auth`: its bootstrap user and provider type
    /// @param publicUrl               `admin.publicUrl`, or `null` when it is not set: then the mcp module cannot be switched on
    /// @param env                     looks up environment variables: the bootstrap user reset switch and the setup token
    public static AdminServices create(
            VirtualTopicManager virtualTopics,
            MetadataCache cache,
            MetadataRefresher metadataRefresher,
            Duration topicVisibilityTimeout,
            DynamicConfigRepository configRepository,
            GovernancePolicy governance,
            BrokerAdmin brokerAdmin,
            SchemaRegistries schemaRegistries,
            AdminAuthConfig auth,
            @Nullable String publicUrl,
            Function<String, @Nullable String> env
    ) {
        new BootstrapUserSeeder(configRepository, Clock.systemUTC())
                .seed(auth.bootstrapUser(), Boolean.parseBoolean(env.apply(BootstrapUserSeeder.RESET_ENV)));
        var adminUsers = new AdminUserService(configRepository);
        return new AdminServices(
                new TopicService(virtualTopics, cache, metadataRefresher, configRepository, brokerAdmin, governance,
                        topicVisibilityTimeout, schemaRegistries),
                new ConsumerGroupService(virtualTopics, brokerAdmin),
                new RbacRoleService(configRepository),
                new RbacGroupService(configRepository),
                new AuthClientService(configRepository),
                new GovernanceService(configRepository, governance),
                adminUsers,
                new AdminUserAuthenticator(configRepository),
                AdminSetup.start(auth, configRepository, adminUsers, env),
                new ModuleService(configRepository, publicUrl),
                new OAuthClientService(configRepository),
                new StoredOAuthClients(configRepository));
    }

    @Override
    public void close() {
        topics.close();
    }
}
