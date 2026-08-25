/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群节点指标总览。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public class ClusterMetricsOverviewVO {

    private double totalTps;
    private long totalFullWorkItems;
    private long totalIncremental;
    private List<ClusterNodeMetricVO> nodes = new ArrayList<>();

    public double getTotalTps() {
        return totalTps;
    }

    public void setTotalTps(double totalTps) {
        this.totalTps = totalTps;
    }

    public long getTotalFullWorkItems() {
        return totalFullWorkItems;
    }

    public void setTotalFullWorkItems(long totalFullWorkItems) {
        this.totalFullWorkItems = totalFullWorkItems;
    }

    public long getTotalIncremental() {
        return totalIncremental;
    }

    public void setTotalIncremental(long totalIncremental) {
        this.totalIncremental = totalIncremental;
    }

    public List<ClusterNodeMetricVO> getNodes() {
        return nodes;
    }

    public void setNodes(List<ClusterNodeMetricVO> nodes) {
        this.nodes = nodes == null ? new ArrayList<>() : nodes;
    }
}
