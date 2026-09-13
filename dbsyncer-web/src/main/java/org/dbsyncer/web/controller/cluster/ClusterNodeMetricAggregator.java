/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import com.alibaba.fastjson2.JSONObject;
import org.dbsyncer.biz.vo.ClusterMetricsOverviewVO;
import org.dbsyncer.biz.vo.ClusterNodeMetricVO;
import org.dbsyncer.biz.vo.HistoryStackVO;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.HttpResult;
import org.dbsyncer.common.util.BatchTaskUtil;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.HttpClientUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UnderlineToCamelUtils;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.storage.ExecuteRequest;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedCaseInsensitiveMap;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 3000;
    private static final int PULL_CONCURRENCY = 8;
    private static final int CHART_HISTORY_COUNT = 12;
    private final HistoryStackVO chartQueue = new HistoryStackVO();
    private final HistoryStackVO chartFullWorkItems = new HistoryStackVO();

    @Resource
    private LocalNodeMetricProvider localNodeMetricProvider;

    @Resource
    private StorageService storageService;

    @Resource
    private ClusterService clusterService;

    @Value("${dbsyncer.cluster.internal-token:}")
    private String internalToken;

    /**
     * 拉取全部节点指标（本机直采，远端 HTTP）。
     *
     * @return 总览
     */
    public ClusterMetricsOverviewVO collectAll() {
        Map<String, String> query = new HashMap<>();
        query.put("pageNum", "1");
        query.put("pageSize", "100");
        List<ClusterNode> nodes = (List<ClusterNode>) clusterService.query(query).getData();
        Map<String, Integer> workItemByNode = resolveFullWorkItemCounts();
        Map<String, Integer> incByNode = resolveIncrementalCounts();
        List<ClusterNode> remotes = new ArrayList<>();
        List<ClusterNodeMetricVO> metrics = new ArrayList<>();
        for (ClusterNode node : nodes) {
            // 如果是本机
            if (node.isLocal()) {
                metrics.add(pullOne(node, workItemByNode, incByNode));
                continue;
            }
            // 远端
            remotes.add(node);
        }
        // 并行获取其他节点信息
        if (!CollectionUtils.isEmpty(remotes)) {
            metrics.addAll(BatchTaskUtil.submit(remotes, node -> pullOne(node, workItemByNode, incByNode), Math.min(PULL_CONCURRENCY, Math.max(1, remotes.size())), logger));
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
        overview.setQueue(snapshotHistory(chartQueue));
        overview.setFullWorkItems(snapshotHistory(chartFullWorkItems));
        pushChartPoint(chartQueue, overview.getTotalQueue());
        pushChartPoint(chartFullWorkItems, overview.getTotalFullWorkItems());
        return overview;
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

    private ClusterNodeMetricVO pullOne(ClusterNode node, Map<String, Integer> workItemByNode, Map<String, Integer> incByNode) {
        ClusterNodeMetricVO vo;
        if (node.isLocal()) {
            vo = localNodeMetricProvider.snapshot();
        } else {
            vo = pullRemote(node);
        }
        vo.setNodeId(node.getNodeId());
        vo.setName(StringUtil.getIfBlank(node.getName(), node.getNodeId()));
        vo.setStatus(node.getStatus());
        vo.setLocal(node.isLocal());
        vo.setIp(node.getIp());
        vo.setHttpPort(node.getHttpPort());
        vo.setFullWorkItemCount(workItemByNode.getOrDefault(node.getNodeId(), 0));
        vo.setIncrementalCount(incByNode.getOrDefault(node.getNodeId(), 0));
        return vo;
    }

    private ClusterNodeMetricVO pullRemote(ClusterNode node) {
        // 已离线
        if (node.getStatus() == 0) {
            return unreachable();
        }
        String base = localNodeMetricProvider.buildHttpUrl(node.getIp(), node.getHttpPort());
        if (StringUtil.isBlank(base)) {
            return unreachable();
        }
        try {
            HttpResult result = HttpClientUtil.get(base + "/cluster/internal/metrics", HttpClientUtil.clusterTokenHeaders(internalToken), CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS);
            if (!result.isOk()) {
                logger.warn("拉取节点指标失败, node={}, http={}", node.getId(), result.getStatusCode());
                return unreachable();
            }
            RestResult res = JsonUtil.jsonToObj(result.getBody(), RestResult.class);
            if (res == null || !res.isSuccess()) {
                return unreachable();
            }
            JSONObject json = (JSONObject) res.getData();
            ClusterNodeMetricVO vo = json.toJavaObject(ClusterNodeMetricVO.class);
            vo.setReachable(true);
            return vo;
        } catch (Exception e) {
            logger.warn("拉取节点指标异常, node={}: {}", node.getId(), e.getMessage());
            return unreachable();
        }
    }

    private ClusterNodeMetricVO unreachable() {
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
        String taskTypeFilter = incrementTask ? " AND TASK_TYPE IN ('increment', 'fullIncrement')" : " AND TASK_TYPE = 'full'";
        try {
            List<Map<String, Object>> rows = storageService.queryList(ExecuteRequest.of("SELECT NODE_ID, COUNT(*) AS CNT FROM " + ConfigConstant.CLUSTER_TASK_TABLE + " WHERE NODE_ID IS NOT NULL" + taskTypeFilter + " GROUP BY NODE_ID"));
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
            String camelKey = keyStr.contains(StringUtil.UNDERLINE) ? UnderlineToCamelUtils.underlineToCamel(keyStr.toLowerCase(), true) : keyStr.toLowerCase();
            result.put(camelKey, value);
        });
        return result;
    }

    private HistoryStackVO mergeTpsSeries(List<ClusterNodeMetricVO> metrics) {
        Map<String, Long> merged = new LinkedHashMap<>();
        List<String> labelOrder = new ArrayList<>();
        for (ClusterNodeMetricVO item : metrics) {
            if (item == null || item.getTpsSeries() == null) {
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
}
