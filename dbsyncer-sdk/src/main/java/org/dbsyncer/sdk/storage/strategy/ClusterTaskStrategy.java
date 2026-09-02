/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 集群任务调度：单表存储 {@code dbsyncer_cluster_task}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-01
 */
public final class ClusterTaskStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.CLUSTER_TASK.getType();
    }
}
