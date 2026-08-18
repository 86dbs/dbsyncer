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
