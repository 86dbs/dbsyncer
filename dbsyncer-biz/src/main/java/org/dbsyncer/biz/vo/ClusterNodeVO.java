/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

/**
 * 集群节点展示。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public class ClusterNodeVO {

    private String id;
    private String name;
    private String ip;
    private int httpPort;
    private int raftPort;
    private int workerId;
    private int role;
    private String roleName;
    private int status;
    private String statusName;
    private boolean networkOk;
    private boolean local;
    private boolean leader;
    private String leaderHttpUrl;
    private long term;
    private long lastHeartbeatTime;
    private long startTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
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

    public String getLeaderHttpUrl() {
        return leaderHttpUrl;
    }

    public void setLeaderHttpUrl(String leaderHttpUrl) {
        this.leaderHttpUrl = leaderHttpUrl;
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
}
