/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.biz.vo.TaskWorkItemSummaryVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.WorkItemAssignment;

import java.util.List;
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

    /**
     * 查询指定节点的工作项（本期无全局派工视图，返回空列表）。
     *
     * @param nodeId 节点 ID
     * @return 派工列表
     */
    List<WorkItemAssignment> listAssignments(String nodeId);

    /**
     * 按任务汇总当前工作项（本期返回空列表）。
     *
     * @return 任务工作项汇总
     */
    List<TaskWorkItemSummaryVO> listTaskWorkItems();

    /**
     * 修改节点展示名称。
     *
     * @param nodeId 节点 ID
     * @param name   展示名称
     */
    void updateNodeName(String nodeId, String name);
}
