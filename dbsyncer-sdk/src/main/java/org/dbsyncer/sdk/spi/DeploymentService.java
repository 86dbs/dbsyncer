/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 部署门面：单机或集群。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface DeploymentService {

    /**
     * 商业集群是否应接管本进程。单机默认 false。
     * 装配层在注册 Bean 前调用（此时尚未 Spring 注入）。
     *
     * @param licenseService 授权
     * @param clusterEnabled dbsyncer.cluster.enabled
     * @param storageType    dbsyncer.storage.type
     * @return true 使用本实现启动 Raft
     */
    default boolean isClusterRuntime(LicenseService licenseService, boolean clusterEnabled, String storageType) {
        return false;
    }

    /**
     * 是否单机。
     *
     * @return true 单机
     */
    boolean isStandalone();

    /**
     * 集群控制面（单机也返回恒 Leader 实现）。
     *
     * @return 控制面
     */
    ClusterService getClusterService();
}
