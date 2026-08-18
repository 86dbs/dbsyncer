/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.DeploymentService;

/**
 * 单机部署。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneProvider implements DeploymentService {

    private final StandaloneClusterService clusterService;

    public StandaloneProvider(MetaProfile metaProfile, ProfileComponent profileComponent) {
        this.clusterService = new StandaloneClusterService(metaProfile, profileComponent);
    }

    @Override
    public boolean isStandalone() {
        return true;
    }

    @Override
    public ClusterService getClusterService() {
        return clusterService;
    }
}
