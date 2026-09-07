/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ClusterManagerService;
import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Collections;
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
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
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
    public void updateNodeName(String nodeId, String name) {
        Assert.hasText(nodeId, "节点ID不能为空");
        Assert.hasText(name, "节点名称不能为空");
        String trimmed = StringUtil.trim(name);
        Assert.isTrue(trimmed.length() <= 64, "节点名称长度不能超过64");
        clusterService.updateNodeName(nodeId, trimmed);
    }

    private ClusterNodeVO toVO(ClusterNode node) {
        ClusterNodeVO vo = new ClusterNodeVO();
        vo.setId(node.getId());
        vo.setNodeId(node.getNodeId());
        vo.setName(node.getName());
        vo.setIp(node.getIp());
        vo.setHttpPort(node.getHttpPort());
        vo.setWorkerId(node.getWorkerId());
        vo.setRole(node.getRole());
        vo.setTerm(node.getTerm());
        vo.setHeartbeatTime(node.getHeartbeatTime());
        vo.setStartTime(node.getStartTime());
        vo.setStatus(node.getStatus());
        vo.setLocal(StringUtil.equals(clusterService.getLocalNodeId(), node.getNodeId()));
        return vo;
    }

}
