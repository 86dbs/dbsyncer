/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.cluster;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 集群配置。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Component
@ConfigurationProperties(prefix = "dbsyncer.cluster")
public class ClusterProperties {

    private boolean enabled;
    private String id = "default";
    private int raftPort;
    private long heartbeatIntervalMs = 3000L;
    private long heartbeatTimeoutMs = 15000L;
    private long leaseTtlMs = 30000L;
    private long drainTimeoutMs = 30000L;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getRaftPort() {
        return raftPort;
    }

    public void setRaftPort(int raftPort) {
        this.raftPort = raftPort;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public void setHeartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public long getLeaseTtlMs() {
        return leaseTtlMs;
    }

    public void setLeaseTtlMs(long leaseTtlMs) {
        this.leaseTtlMs = leaseTtlMs;
    }

    public long getDrainTimeoutMs() {
        return drainTimeoutMs;
    }

    public void setDrainTimeoutMs(long drainTimeoutMs) {
        this.drainTimeoutMs = drainTimeoutMs;
    }
}
