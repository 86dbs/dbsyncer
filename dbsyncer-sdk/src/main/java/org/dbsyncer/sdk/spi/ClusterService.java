/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.ClusterNode;

import java.util.Collections;
import java.util.List;

/**
 * 集群服务
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterService {

    default void init() {
    }

    default boolean isStandalone() {
        return true;
    }

    default String getLocalNodeId() {
        return "standalone";
    }

    default List<ClusterNode> listNodes() {
        return Collections.emptyList();
    }

    default Paging<ClusterNode> queryNodes(int pageNum, int pageSize) {
        return null;
    }

    default void removeNode(String nodeId) {
        throw new SdkException("单机模式不支持移除节点");
    }

    default void updateNodeName(String nodeId, String name) {
        throw new SdkException("单机模式不支持修改节点名称");
    }

}
