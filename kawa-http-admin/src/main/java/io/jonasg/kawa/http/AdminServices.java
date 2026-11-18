package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig.BootstrapUser;
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
import io.jonasg.kawa.http.users.AdminUserAuthenticator;
import io.jonasg.kawa.http.users.AdminUserService;
import io.jonasg.kawa.http.users.BootstrapUserSeeder;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
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
public record AdminServices(
        TopicService topics,
        ConsumerGroupService consumerGroups,
        RbacRoleService roles,
        RbacGroupService groups,
        AuthClientService clients,
        GovernanceService governanceRules,
        AdminUserService adminUsers,
        AdminUserAuthenticator adminUserAuthenticator,
        ModuleService modules,
        OAuthClientService oauthClients,
        StoredOAuthClients oauthClientDirectory
) implements AutoCloseable {

    /// Builds the services and seeds `bootstrapUser` into the admin users (see
    /// [BootstrapUserSeeder]), so it must be called after the config topic has been read and
    /// before the admin authentication provider is created: a provider may require an admin user
    /// to exist.
    ///
    /// @param metadataRefresher       refreshes `cache` on demand, so a created topic is listed at once
    /// @param topicVisibilityTimeout  how long a topic creation waits for the created topic to reach `cache`
    /// @param publicUrl               `admin.publicUrl`, or `null` when it is not set: then the mcp module cannot be switched on
    /// @param env                     looks up environment variables, such as the bootstrap user reset switch
    public static AdminServices create(
            VirtualTopicManager virtualTopics,
            MetadataCache cache,
            MetadataRefresher metadataRefresher,
            Duration topicVisibilityTimeout,
            DynamicConfigRepository configRepository,
            GovernancePolicy governance,
            BrokerAdmin brokerAdmin,
            @Nullable BootstrapUser bootstrapUser,
            @Nullable String publicUrl,
            Function<String, @Nullable String> env
    ) {
        new BootstrapUserSeeder(configRepository, Clock.systemUTC())
                .seed(bootstrapUser, Boolean.parseBoolean(env.apply(BootstrapUserSeeder.RESET_ENV)));
        return new AdminServices(
                new TopicService(virtualTopics, cache, metadataRefresher, configRepository, brokerAdmin, governance,
                        topicVisibilityTimeout),
                new ConsumerGroupService(virtualTopics, brokerAdmin),
                new RbacRoleService(configRepository),
                new RbacGroupService(configRepository),
                new AuthClientService(configRepository),
                new GovernanceService(configRepository, governance),
                new AdminUserService(configRepository),
                new AdminUserAuthenticator(configRepository),
                new ModuleService(configRepository, publicUrl),
                new OAuthClientService(configRepository),
                new StoredOAuthClients(configRepository));
    }

    @Override
    public void close() {
        topics.close();
    }
}
