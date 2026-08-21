/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.monitor;

import org.dbsyncer.biz.ClusterManagerService;
import org.dbsyncer.biz.vo.ClusterMetricsOverviewVO;
import org.dbsyncer.biz.vo.ClusterNodeMetricVO;
import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.biz.vo.TaskShardSummaryVO;
import org.dbsyncer.common.util.BatchTaskUtil;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NetUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 通过各节点 HTTP 地址拉取运行指标并聚合。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
@Service
public class ClusterNodeMetricAggregator {

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 3000;
    private static final int PULL_CONCURRENCY = 8;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ClusterService clusterService;

    @Resource
    private ClusterManagerService clusterManagerService;

    @Resource
    private LocalNodeMetricProvider localNodeMetricProvider;

    /**
     * 拉取全部在册节点指标（本机直采，远端 HTTP）。
     *
     * @return 总览
     */
    public ClusterMetricsOverviewVO collectAll() {
        Map<String, String> query = new HashMap<>();
        query.put("pageNum", "1");
        query.put("pageSize", "1000");
        List<ClusterNodeVO> nodes = new ArrayList<>();
        Collection<ClusterNodeVO> raw = clusterManagerService.query(query).getData();
        if (!CollectionUtils.isEmpty(raw)) {
            nodes.addAll(raw);
        }
        if (CollectionUtils.isEmpty(nodes)) {
            ClusterMetricsOverviewVO empty = new ClusterMetricsOverviewVO();
            ClusterNodeMetricVO local = localNodeMetricProvider.snapshot();
            local.setName(local.getNodeId());
            local.setRoleName(clusterService.getRole().name());
            empty.setNodes(Collections.singletonList(local));
            empty.setTotalTps(local.getTps());
            return empty;
        }
        Map<String, Integer> shardByNode = resolveFullShardCounts();
        List<ClusterNodeMetricVO> metrics = BatchTaskUtil.submit(nodes, node -> pullOne(node, shardByNode),
                Math.min(PULL_CONCURRENCY, Math.max(1, nodes.size())), logger);
        ClusterMetricsOverviewVO overview = new ClusterMetricsOverviewVO();
        double totalTps = 0D;
        long totalShards = 0L;
        long totalInc = 0L;
        for (ClusterNodeMetricVO item : metrics) {
            if (item == null) {
                continue;
            }
            overview.getNodes().add(item);
            if (item.isReachable()) {
                totalTps += item.getTps();
            }
            totalShards += item.getFullShardCount();
            totalInc += item.getIncrementalCount();
        }
        overview.setTotalTps(Math.floor(totalTps));
        overview.setTotalFullShards(totalShards);
        overview.setTotalIncremental(totalInc);
        return overview;
    }

    private ClusterNodeMetricVO pullOne(ClusterNodeVO node, Map<String, Integer> shardByNode) {
        ClusterNodeMetricVO vo;
        if (node.isLocal()) {
            vo = localNodeMetricProvider.snapshot();
        } else {
            vo = pullRemote(node);
        }
        fillIdentity(vo, node);
        vo.setFullShardCount(shardByNode.getOrDefault(node.getId(), 0));
        return vo;
    }

    private void fillIdentity(ClusterNodeMetricVO vo, ClusterNodeVO node) {
        vo.setNodeId(node.getId());
        vo.setName(StringUtil.getIfBlank(node.getName(), node.getId()));
        vo.setRoleName(node.getRoleName());
        vo.setStatusName(node.getStatusName());
        vo.setNetworkOk(node.isNetworkOk());
        vo.setLocal(node.isLocal());
        vo.setLeader(node.isLeader());
        vo.setIp(node.getIp());
        vo.setHttpPort(node.getHttpPort());
    }

    @SuppressWarnings("unchecked")
    private ClusterNodeMetricVO pullRemote(ClusterNodeVO node) {
        String base = localNodeMetricProvider.buildHttpUrl(node.getIp(), node.getHttpPort());
        if (StringUtil.isBlank(base)) {
            return unreachable(node);
        }
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(base + "/cluster/metrics").openConnection();
            NetUtil.applyInsecureSslIfNeeded(connection);
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);
            if (connection.getResponseCode() != 200) {
                logger.warn("拉取节点指标失败, node={}, http={}", node.getId(), connection.getResponseCode());
                return unreachable(node);
            }
            String body = readBody(connection);
            Map<String, Object> root = JsonUtil.jsonToObj(body, Map.class);
            if (root == null || !Boolean.TRUE.equals(root.get("success")) || root.get("data") == null) {
                return unreachable(node);
            }
            String json = root.get("data") instanceof String
                    ? (String) root.get("data")
                    : JsonUtil.objToJson(root.get("data"));
            ClusterNodeMetricVO vo = JsonUtil.jsonToObj(json, ClusterNodeMetricVO.class);
            if (vo == null) {
                return unreachable(node);
            }
            vo.setReachable(true);
            return vo;
        } catch (Exception e) {
            logger.warn("拉取节点指标异常, node={}: {}", node.getId(), e.getMessage());
            return unreachable(node);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private ClusterNodeMetricVO unreachable(ClusterNodeVO node) {
        ClusterNodeMetricVO vo = new ClusterNodeMetricVO();
        vo.setReachable(false);
        vo.setCpuPercent(BigDecimal.ZERO);
        vo.setMemoryUsed(BigDecimal.ZERO);
        vo.setMemoryTotal(BigDecimal.ZERO);
        vo.setDiskUsed(BigDecimal.ZERO);
        vo.setDiskTotal(BigDecimal.ZERO);
        return vo;
    }

    private Map<String, Integer> resolveFullShardCounts() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        if (!clusterService.isLeader()) {
            return counts;
        }
        try {
            List<TaskShardSummaryVO> shards = clusterManagerService.listTaskShards();
            if (CollectionUtils.isEmpty(shards)) {
                return counts;
            }
            for (TaskShardSummaryVO item : shards) {
                String dist = item == null ? null : item.getNodeDistribution();
                if (StringUtil.isBlank(dist) || StringUtil.equals("-", dist)) {
                    continue;
                }
                for (String part : dist.split(",")) {
                    String seg = StringUtil.trim(part);
                    if (StringUtil.isBlank(seg)) {
                        continue;
                    }
                    int idx = seg.lastIndexOf(':');
                    if (idx <= 0) {
                        continue;
                    }
                    String nodeId = StringUtil.trim(seg.substring(0, idx));
                    int num;
                    try {
                        num = Integer.parseInt(StringUtil.trim(seg.substring(idx + 1)));
                    } catch (NumberFormatException ex) {
                        continue;
                    }
                    if (StringUtil.isBlank(nodeId)) {
                        continue;
                    }
                    counts.merge(nodeId, num, Integer::sum);
                }
            }
        } catch (Exception e) {
            logger.warn("汇总分片数失败: {}", e.getMessage());
        }
        return counts;
    }

    private static String readBody(HttpURLConnection connection) throws Exception {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }
}
