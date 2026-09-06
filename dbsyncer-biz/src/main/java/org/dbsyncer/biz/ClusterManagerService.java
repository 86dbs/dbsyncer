/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.ClusterNode;

import java.util.Map;

/**
 * 集群管理
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterManagerService {

    /**
     * 是否集群模式
     */
    boolean isClusterEnabled();

    /**
     * 分页查询节点
     */
    Paging<ClusterNodeVO> query(Map<String, String> params);

    /**
     * 本节点信息
     */
    ClusterNodeVO current();

    /**
     * 修改节点名称
     *
     * @param nodeId 节点 ID
     * @param name   展示名称
     */
    void updateNodeName(String nodeId, String name);
}
