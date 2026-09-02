/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * 集群管理。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterManagerService {

    /**
     * 是否集群模式。
     *
     * @return true 集群
     */
    boolean isClusterEnabled();

    /**
     * 分页查询节点。
     *
     * @param params 查询参数
     * @return 分页
     */
    Paging<ClusterNodeVO> query(Map<String, String> params);

    /**
     * 本节点摘要。
     *
     * @return 节点
     */
    ClusterNodeVO current();

    /**
     * 修改节点展示名称。
     *
     * @param nodeId 节点 ID
     * @param name   展示名称
     */
    void updateNodeName(String nodeId, String name);
}
