/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ClusterManagerService;
import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.biz.vo.TaskWorkItemSummaryVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.ClusterNodeStatusEnum;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.model.WorkItemAssignment;
import org.dbsyncer.sdk.model.WorkItemIds;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 集群管理。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Service
public class ClusterManagerServiceImpl implements ClusterManagerService {

    @Resource
    private ClusterService clusterService;

    @Override
    public boolean isClusterEnabled() {
        return !clusterService.isStandalone();
    }

    @Override
    public Paging<ClusterNodeVO> query(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params == null ? null : params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params == null ? null : params.get("pageSize"), 10);
        Paging<ClusterNode> source = clusterService.queryNodes(pageNum, pageSize);
        Paging<ClusterNodeVO> paging = new Paging<>(pageNum, pageSize);
        paging.setTotal(source.getTotal());
        if (CollectionUtils.isEmpty(source.getData())) {
            paging.setData(Collections.emptyList());
            return paging;
        }
        paging.setData(source.getData().stream().map(this::toVO).collect(Collectors.toList()));
        return paging;
    }

    @Override
    public void transferLeadership(String nodeId) {
        Assert.hasText(nodeId, "节点ID不能为空");
        clusterService.transferLeadership(nodeId);
    }

    @Override
    public void removeNode(String nodeId) {
        Assert.hasText(nodeId, "节点ID不能为空");
        clusterService.removeNode(nodeId);
    }

    @Override
    public ClusterNodeVO current() {
        ClusterNodeVO vo = new ClusterNodeVO();
        vo.setId(clusterService.getLocalNodeId());
        vo.setLeader(clusterService.isLeader());
        vo.setRoleName(clusterService.getRole().name());
        vo.setLocal(true);
        vo.setLeaderHttpUrl(clusterService.getLeaderHttpUrl());
        return vo;
    }

    @Override
    public List<WorkItemAssignment> listAssignments(String nodeId) {
        Assert.hasText(nodeId, "节点ID不能为空");
        List<WorkItemAssignment> list = clusterService.listAssignments(nodeId);
        return list == null ? Collections.emptyList() : list;
    }

    @Override
    public void updateNodeName(String nodeId, String name) {
        Assert.hasText(nodeId, "节点ID不能为空");
        Assert.hasText(name, "节点名称不能为空");
        String trimmed = StringUtil.trim(name);
        Assert.isTrue(trimmed.length() <= 64, "节点名称长度不能超过64");
        clusterService.updateNodeName(nodeId, trimmed);
    }

    @Override
    public List<TaskWorkItemSummaryVO> listTaskWorkItems() {
        List<WorkItemAssignment> all = clusterService.listAllAssignments();
        if (all == null || all.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, TaskWorkItemSummaryVO> byTask = new LinkedHashMap<>();
        Map<String, Map<String, Integer>> nodeCounts = new LinkedHashMap<>();
        for (WorkItemAssignment item : all) {
            if (item == null || StringUtil.isBlank(item.getTaskId())) {
                continue;
            }
            TaskWorkItemSummaryVO vo = byTask.computeIfAbsent(item.getTaskId(), id -> {
                TaskWorkItemSummaryVO created = new TaskWorkItemSummaryVO();
                created.setTaskId(id);
                return created;
            });
            if (WorkItemIds.isTaskLevelItem(item.getTaskId(), item.getItemId())) {
                vo.setIncrementTask(true);
            }
            vo.setWorkItemCount(vo.getWorkItemCount() + 1);
            if (item.getGeneration() > vo.getMaxGeneration()) {
                vo.setMaxGeneration(item.getGeneration());
            }
            String nodeId = StringUtil.getIfBlank(item.getNodeId(), "-");
            nodeCounts.computeIfAbsent(item.getTaskId(), k -> new LinkedHashMap<>())
                    .merge(nodeId, 1, Integer::sum);
        }
        List<TaskWorkItemSummaryVO> result = new ArrayList<>(byTask.values());
        for (TaskWorkItemSummaryVO vo : result) {
            Map<String, Integer> counts = nodeCounts.get(vo.getTaskId());
            if (counts == null || counts.isEmpty()) {
                vo.setNodeDistribution("-");
                continue;
            }
            vo.setNodeDistribution(counts.entrySet().stream()
                    .map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.joining(", ")));
        }
        return result;
    }

    private ClusterNodeVO toVO(ClusterNode node) {
        ClusterNodeVO vo = new ClusterNodeVO();
        vo.setId(node.getNodeId());
        vo.setName(node.getName());
        vo.setIp(node.getIp());
        vo.setHttpPort(node.getHttpPort());
        vo.setRaftPort(node.getRaftPort());
        vo.setWorkerId(node.getWorkerId());
        vo.setRole(node.getRole());
        boolean leader = StringUtil.equals(clusterService.getLeaderId(), node.getNodeId());
        vo.setLeader(leader);
        vo.setRoleName(leader ? "Leader" : "Follower");
        vo.setStatus(node.getStatus());
        vo.setStatusName(statusName(node.getStatus()));
        vo.setNetworkOk(node.getNetworkOk() == 1);
        vo.setLocal(StringUtil.equals(clusterService.getLocalNodeId(), node.getNodeId()));
        vo.setTerm(node.getTerm());
        vo.setLastHeartbeatTime(node.getLastHeartbeatTime());
        vo.setStartTime(node.getStartTime());
        return vo;
    }

    private String statusName(int status) {
        ClusterNodeStatusEnum e = ClusterNodeStatusEnum.fromCode(status);
        switch (e) {
            case JOINING:
                return "加入中";
            case ONLINE:
                return "在线";
            case UNREACHABLE:
                return "网络不通";
            case LEAVING:
                return "退出中";
            default:
                return "离线";
        }
    }
}
