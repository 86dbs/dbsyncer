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
     * 转让 Leader。
     *
     * @param nodeId 目标节点
     */
    void transferLeadership(String nodeId);

    /**
     * 移除节点。
     *
     * @param nodeId 节点
     */
    void removeNode(String nodeId);

    /**
     * 本节点摘要。
     *
     * @return 节点
     */
    ClusterNodeVO current();
}
