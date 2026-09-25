package io.jonasg.kawa.server.auth;

import software.amazon.awssdk.regions.Region;
import software.amazon.msk.auth.iam.internals.region.ConfigurableRegionProvider;

import java.util.Objects;

final class FixedRegionProvider implements ConfigurableRegionProvider {

    private final Region region;

    FixedRegionProvider(String region) {
        this.region = Region.of(Objects.requireNonNull(region));
    }

    @Override
    public Region getRegion() {
        return region;
    }

    @Override
    public Region getRegion(String host) {
        return region;
    }
}
