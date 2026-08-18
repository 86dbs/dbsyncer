/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

/**
 * 集群节点（对应 {@code dbsyncer_cluster_node}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public class ClusterNode {

    /**
     * 表自增主键。
     */
    private String id;
    /**
     * 节点业务 ID，{ip}:{httpPort}。
     */
    private String nodeId;
    private String clusterId;
    private String name;
    private String ip;
    private int httpPort;
    private int raftPort;
    private String raftPeerId;
    private int workerId;
    private int role;
    private int status;
    private int networkOk;
    private long term;
    private long lastHeartbeatTime;
    private long startTime;
    private long createTime;
    private long updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public int getRaftPort() {
        return raftPort;
    }

    public void setRaftPort(int raftPort) {
        this.raftPort = raftPort;
    }

    public String getRaftPeerId() {
        return raftPeerId;
    }

    public void setRaftPeerId(String raftPeerId) {
        this.raftPeerId = raftPeerId;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public int getRole() {
        return role;
    }

    public void setRole(int role) {
        this.role = role;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getNetworkOk() {
        return networkOk;
    }

    public void setNetworkOk(int networkOk) {
        this.networkOk = networkOk;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public void setLastHeartbeatTime(long lastHeartbeatTime) {
        this.lastHeartbeatTime = lastHeartbeatTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }
}
