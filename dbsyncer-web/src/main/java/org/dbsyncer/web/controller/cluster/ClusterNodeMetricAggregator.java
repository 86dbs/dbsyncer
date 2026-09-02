/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import org.dbsyncer.biz.ClusterManagerService;
import org.dbsyncer.biz.vo.ClusterMetricsOverviewVO;
import org.dbsyncer.biz.vo.ClusterNodeMetricVO;
import org.dbsyncer.biz.vo.ClusterNodeVO;
import org.dbsyncer.biz.vo.HistoryStackVO;
import org.dbsyncer.common.util.BatchTaskUtil;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NetUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UnderlineToCamelUtils;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.storage.SqlQuery;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedCaseInsensitiveMap;

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
    private static final int CHART_HISTORY_COUNT = 12;

    private final HistoryStackVO chartQueue = new HistoryStackVO();
    private final HistoryStackVO chartFullWorkItems = new HistoryStackVO();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ClusterManagerService clusterManagerService;

    @Resource
    private LocalNodeMetricProvider localNodeMetricProvider;

    @Resource
    private StorageService storageService;

    @Resource
    private ClusterService clusterService;

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
            local.setRoleName("");
            empty.setNodes(Collections.singletonList(local));
            empty.setTotalTps(Math.floor(local.getTps()));
            empty.setTotalQueue(local.getQueueUp());
            empty.setTotalFullWorkItems(local.getFullWorkItemCount());
            empty.setTotalIncremental(local.getIncrementalCount());
            empty.setTps(mergeTpsSeries(Collections.singletonList(local)));
            recordChartMetrics(empty);
            return empty;
        }
        Map<String, Integer> workItemByNode = resolveFullWorkItemCounts();
        Map<String, Integer> incByNode = resolveIncrementalCounts();
        List<ClusterNodeVO> remotes = new ArrayList<>();
        List<ClusterNodeMetricVO> metrics = new ArrayList<>();
        for (ClusterNodeVO node : nodes) {
            if (node.isLocal()) {
                metrics.add(pullOne(node, workItemByNode, incByNode));
            } else {
                remotes.add(node);
            }
        }
        if (remotes.size() == 1) {
            metrics.add(pullOne(remotes.get(0), workItemByNode, incByNode));
        } else if (!CollectionUtils.isEmpty(remotes)) {
            metrics.addAll(BatchTaskUtil.submit(remotes, node -> pullOne(node, workItemByNode, incByNode),
                    Math.min(PULL_CONCURRENCY, Math.max(1, remotes.size())), logger));
        }
        ClusterMetricsOverviewVO overview = new ClusterMetricsOverviewVO();
        double totalTps = 0D;
        long totalQueue = 0L;
        long totalWorkItems = 0L;
        long totalInc = 0L;
        for (ClusterNodeMetricVO item : metrics) {
            if (item == null) {
                continue;
            }
            overview.getNodes().add(item);
            if (item.isReachable()) {
                totalTps += item.getTps();
                totalQueue += item.getQueueUp();
            }
            totalWorkItems += item.getFullWorkItemCount();
            totalInc += item.getIncrementalCount();
        }
        overview.setTotalTps(Math.floor(totalTps));
        overview.setTotalQueue(totalQueue);
        overview.setTotalFullWorkItems(totalWorkItems);
        overview.setTotalIncremental(totalInc);
        overview.setTps(mergeTpsSeries(metrics));
        recordChartMetrics(overview);
        return overview;
    }

    private void recordChartMetrics(ClusterMetricsOverviewVO overview) {
        pushChartPoint(chartQueue, overview.getTotalQueue());
        pushChartPoint(chartFullWorkItems, overview.getTotalFullWorkItems());
        fillChartHistory(overview);
    }

    private void fillChartHistory(ClusterMetricsOverviewVO overview) {
        overview.setQueue(snapshotHistory(chartQueue));
        overview.setFullWorkItems(snapshotHistory(chartFullWorkItems));
    }

    private void pushChartPoint(HistoryStackVO history, double value) {
        history.addName(DateFormatUtil.getCurrentTime());
        history.addValue(value);
        while (history.getName().size() > CHART_HISTORY_COUNT) {
            history.getName().remove(0);
            history.getValue().remove(0);
        }
        history.setAverage(average(history.getValue()));
    }

    private HistoryStackVO snapshotHistory(HistoryStackVO source) {
        HistoryStackVO snapshot = new HistoryStackVO();
        snapshot.setName(new ArrayList<>(source.getName()));
        snapshot.setValue(new ArrayList<>(source.getValue()));
        snapshot.setAverage(source.getAverage());
        return snapshot;
    }

    private double average(List<Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return 0D;
        }
        double sum = 0D;
        for (Object value : values) {
            if (value instanceof Number) {
                sum += ((Number) value).doubleValue();
            }
        }
        return Math.floor(sum / values.size());
    }

    private ClusterNodeMetricVO pullOne(ClusterNodeVO node, Map<String, Integer> workItemByNode,
                                        Map<String, Integer> incByNode) {
        ClusterNodeMetricVO vo;
        if (node.isLocal()) {
            vo = localNodeMetricProvider.snapshot();
        } else {
            vo = pullRemote(node);
        }
        fillIdentity(vo, node);
        vo.setFullWorkItemCount(workItemByNode.getOrDefault(node.getId(), 0));
        vo.setIncrementalCount(incByNode.getOrDefault(node.getId(), 0));
        return vo;
    }

    private void fillIdentity(ClusterNodeMetricVO vo, ClusterNodeVO node) {
        vo.setNodeId(node.getId());
        vo.setName(StringUtil.getIfBlank(node.getName(), node.getId()));
        vo.setStatusName(node.getStatusName());
        vo.setNetworkOk(node.isNetworkOk());
        vo.setLocal(node.isLocal());
        vo.setLeader(node.isLeader());
        vo.setRoleName(node.isLeader() ? "Leader" : "");
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

    private Map<String, Integer> resolveFullWorkItemCounts() {
        return resolveAssignmentCounts(false);
    }

    private Map<String, Integer> resolveIncrementalCounts() {
        return resolveAssignmentCounts(true);
    }

    private Map<String, Integer> resolveAssignmentCounts(boolean incrementTask) {
        if (clusterService.isStandalone()) {
            return new LinkedHashMap<>();
        }
        String taskTypeFilter = incrementTask
                ? " AND TASK_TYPE IN ('increment', 'fullIncrement')"
                : " AND TASK_TYPE = 'full'";
        try {
            List<Map<String, Object>> rows = storageService.queryList(SqlQuery.of(
                    "SELECT NODE_ID, COUNT(*) AS CNT FROM " + ConfigConstant.CLUSTER_TASK_TABLE
                            + " WHERE NODE_ID IS NOT NULL" + taskTypeFilter
                            + " GROUP BY NODE_ID"));
            return toNodeCountMap(rows);
        } catch (Exception e) {
            logger.warn("加载集群任务派工统计失败: {}", e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    private Map<String, Integer> toNodeCountMap(List<Map<String, Object>> rows) {
        Map<String, Integer> result = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(rows)) {
            return result;
        }
        for (Map<String, Object> row : rows) {
            Map<String, Object> normalized = normalizeRow(row);
            String nodeId = String.valueOf(normalized.getOrDefault(ConfigConstant.SCHEDULE_NODE_ID, ""));
            if (StringUtil.isBlank(nodeId)) {
                continue;
            }
            result.put(nodeId, NumberUtil.toInt(String.valueOf(normalized.get("cnt")), 0));
        }
        return result;
    }

    private Map<String, Object> normalizeRow(Map<String, Object> row) {
        Map<String, Object> result = new LinkedCaseInsensitiveMap<>();
        if (row == null) {
            return result;
        }
        row.forEach((key, value) -> {
            String keyStr = key == null ? StringUtil.EMPTY : String.valueOf(key);
            String camelKey = keyStr.contains(StringUtil.UNDERLINE)
                    ? UnderlineToCamelUtils.underlineToCamel(keyStr.toLowerCase(), true)
                    : keyStr.toLowerCase();
            result.put(camelKey, value);
        });
        return result;
    }

    private HistoryStackVO mergeTpsSeries(List<ClusterNodeMetricVO> metrics) {
        Map<String, Long> merged = new LinkedHashMap<>();
        List<String> labelOrder = new ArrayList<>();
        for (ClusterNodeMetricVO item : metrics) {
            if (item == null || !item.isReachable() || item.getTpsSeries() == null) {
                continue;
            }
            HistoryStackVO series = item.getTpsSeries();
            List<Object> names = series.getName();
            List<Object> values = series.getValue();
            if (CollectionUtils.isEmpty(names) || CollectionUtils.isEmpty(values)) {
                continue;
            }
            if (labelOrder.isEmpty()) {
                for (Object name : names) {
                    labelOrder.add(String.valueOf(name));
                }
            }
            int size = Math.min(names.size(), values.size());
            for (int i = 0; i < size; i++) {
                String key = String.valueOf(names.get(i));
                long value = values.get(i) instanceof Number ? ((Number) values.get(i)).longValue() : 0L;
                merged.merge(key, value, Long::sum);
            }
        }
        HistoryStackVO result = new HistoryStackVO();
        for (String label : labelOrder) {
            result.addName(label);
            result.addValue(merged.getOrDefault(label, 0L));
        }
        result.setAverage(averageSeries(result.getValue()));
        return result;
    }

    private double averageSeries(List<Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return 0D;
        }
        long total = 0L;
        for (Object value : values) {
            if (value instanceof Number) {
                total += ((Number) value).longValue();
            }
        }
        return Math.floor((double) total / values.size());
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
