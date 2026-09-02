/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.math.BigDecimal;

/**
 * 集群节点运行指标（本机采集或 HTTP 拉取）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public class ClusterNodeMetricVO {

    private String nodeId;
    private String name;
    private String roleName;
    private String statusName;
    private boolean networkOk;
    private boolean local;
    private boolean leader;
    private String ip;
    private int httpPort;
    /**
     * 指标是否拉取成功。
     */
    private boolean reachable;
    private BigDecimal cpuPercent;
    private BigDecimal memoryUsed;
    private BigDecimal memoryTotal;
    private long threadLive;
    private BigDecimal diskUsed;
    private BigDecimal diskTotal;
    private double tps;
    /**
     * 近 1 分钟每秒吞吐序列，供集群页聚合图表。
     */
    private HistoryStackVO tpsSeries;
    private long queueUp;
    private long storageQueueUp;
    private int fullWorkItemCount;
    private int incrementalCount;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public String getStatusName() {
        return statusName;
    }

    public void setStatusName(String statusName) {
        this.statusName = statusName;
    }

    public boolean isNetworkOk() {
        return networkOk;
    }

    public void setNetworkOk(boolean networkOk) {
        this.networkOk = networkOk;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public boolean isLeader() {
        return leader;
    }

    public void setLeader(boolean leader) {
        this.leader = leader;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public boolean isReachable() {
        return reachable;
    }

    public void setReachable(boolean reachable) {
        this.reachable = reachable;
    }

    public BigDecimal getCpuPercent() {
        return cpuPercent;
    }

    public void setCpuPercent(BigDecimal cpuPercent) {
        this.cpuPercent = cpuPercent;
    }

    public BigDecimal getMemoryUsed() {
        return memoryUsed;
    }

    public void setMemoryUsed(BigDecimal memoryUsed) {
        this.memoryUsed = memoryUsed;
    }

    public BigDecimal getMemoryTotal() {
        return memoryTotal;
    }

    public void setMemoryTotal(BigDecimal memoryTotal) {
        this.memoryTotal = memoryTotal;
    }

    public long getThreadLive() {
        return threadLive;
    }

    public void setThreadLive(long threadLive) {
        this.threadLive = threadLive;
    }

    public BigDecimal getDiskUsed() {
        return diskUsed;
    }

    public void setDiskUsed(BigDecimal diskUsed) {
        this.diskUsed = diskUsed;
    }

    public BigDecimal getDiskTotal() {
        return diskTotal;
    }

    public void setDiskTotal(BigDecimal diskTotal) {
        this.diskTotal = diskTotal;
    }

    public double getTps() {
        return tps;
    }

    public void setTps(double tps) {
        this.tps = tps;
    }

    public HistoryStackVO getTpsSeries() {
        return tpsSeries;
    }

    public void setTpsSeries(HistoryStackVO tpsSeries) {
        this.tpsSeries = tpsSeries;
    }

    public long getQueueUp() {
        return queueUp;
    }

    public void setQueueUp(long queueUp) {
        this.queueUp = queueUp;
    }

    public long getStorageQueueUp() {
        return storageQueueUp;
    }

    public void setStorageQueueUp(long storageQueueUp) {
        this.storageQueueUp = storageQueueUp;
    }

    public int getFullWorkItemCount() {
        return fullWorkItemCount;
    }

    public void setFullWorkItemCount(int fullWorkItemCount) {
        this.fullWorkItemCount = fullWorkItemCount;
    }

    public int getIncrementalCount() {
        return incrementalCount;
    }

    public void setIncrementalCount(int incrementalCount) {
        this.incrementalCount = incrementalCount;
    }
}
