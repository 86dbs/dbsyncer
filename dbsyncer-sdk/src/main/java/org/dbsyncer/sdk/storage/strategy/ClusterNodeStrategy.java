/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 集群节点：单表存储 {@code dbsyncer_cluster_node}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class ClusterNodeStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.CLUSTER_NODE.getType();
    }
}
