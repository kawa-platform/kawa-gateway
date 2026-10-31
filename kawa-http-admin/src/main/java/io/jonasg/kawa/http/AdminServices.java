package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig.BootstrapUser;
import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.http.auth.AdminUserAuthenticator;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;

import java.time.Clock;
import java.util.function.Function;

/// The services behind the admin surface, built once and shared by every surface that exposes
/// them (the [AdminHttpServer] routes). Closing them closes the [TopicAdmin] they were built with,
/// so close them only after every surface has stopped.
///
/// @param adminUserAuthenticator checks admin user credentials for the authentication provider
public record AdminServices(
        TopicService topics,
        RbacRoleService roles,
        RbacGroupService groups,
        AuthClientService clients,
        GovernanceService governanceRules,
        AdminUserService adminUsers,
        AdminUserAuthenticator adminUserAuthenticator,
        ModuleService modules
) implements AutoCloseable {

    /// Builds the services and seeds `bootstrapUser` into the admin users (see
    /// [BootstrapUserSeeder]), so it must be called after the config topic has been read and
    /// before the admin authentication provider is created: a provider may require an admin user
    /// to exist.
    ///
    /// @param env looks up environment variables, such as the bootstrap user reset switch
    public static AdminServices create(
            VirtualTopicManager virtualTopics,
            MetadataCache cache,
            DynamicConfigRepository configRepository,
            GovernancePolicy governance,
            TopicAdmin topicAdmin,
            @Nullable BootstrapUser bootstrapUser,
            Function<String, @Nullable String> env
    ) {
        new BootstrapUserSeeder(configRepository, Clock.systemUTC())
                .seed(bootstrapUser, Boolean.parseBoolean(env.apply(BootstrapUserSeeder.RESET_ENV)));
        return new AdminServices(
                new TopicService(virtualTopics, cache, configRepository, topicAdmin, governance),
                new RbacRoleService(configRepository),
                new RbacGroupService(configRepository),
                new AuthClientService(configRepository),
                new GovernanceService(configRepository, governance),
                new AdminUserService(configRepository),
                new AdminUserAuthenticator(configRepository),
                new ModuleService(configRepository));
    }

    @Override
    public void close() {
        topics.close();
    }
}
