/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 部署服务
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface DeploymentService {

    default boolean isClusterRuntime(LicenseService licenseService, boolean clusterEnabled, String storageType) {
        return false;
    }

    boolean isStandalone();

    ClusterService getClusterService();
}
