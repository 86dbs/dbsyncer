/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.ClusterRoleEnum;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.LeaderLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 单机控制面：恒为 Leader，租约本地恒成功。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneClusterService implements ClusterService {

    public static final String NODE_ID = "standalone";

    private static final long LEASE_TTL_MS = 24L * 60 * 60 * 1000;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<LeaderLifecycleListener> listeners = new CopyOnWriteArrayList<>();
    private final MetaProfile metaProfile;

    public StandaloneClusterService(MetaProfile metaProfile) {
        this.metaProfile = metaProfile;
    }

    @Override
    public boolean isStandalone() {
        return true;
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public String getLocalNodeId() {
        return NODE_ID;
    }

    @Override
    public String getLeaderId() {
        return NODE_ID;
    }

    @Override
    public String getLeaderHttpUrl() {
        return "";
    }

    @Override
    public ClusterRoleEnum getRole() {
        return ClusterRoleEnum.LEADER;
    }

    @Override
    public List<ClusterNode> listNodes() {
        return Collections.emptyList();
    }

    @Override
    public void assertLeaderWritable() {
    }

    @Override
    public boolean tryAcquireLease(String metaId) {
        return persistLease(metaId, NODE_ID);
    }

    @Override
    public boolean assignLease(String metaId, String ownerNodeId) {
        return persistLease(metaId, ownerNodeId);
    }

    @Override
    public void releaseLease(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return;
        }
        metaProfile.compareAndSetLease(meta.getId(), meta.getEpoch(), null, 0L);
    }

    @Override
    public boolean hasValidLease(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        return NODE_ID.equals(meta.getLeaseOwner()) && meta.getLeaseExpireAt() > now;
    }

    @Override
    public void addLeaderListener(LeaderLifecycleListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    /**
     * 单机启动后通知一次升主（供任务恢复挂钩）。
     */
    public void notifyStandaloneLeader() {
        for (LeaderLifecycleListener listener : listeners) {
            try {
                listener.onLeaderStart(1L);
            } catch (Exception e) {
                logger.error("单机 Leader 回调失败: {}", e.getMessage(), e);
            }
        }
    }

    private boolean persistLease(String metaId, String ownerNodeId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            throw new SdkException("任务运行态不存在: " + metaId);
        }
        long expireAt = System.currentTimeMillis() + LEASE_TTL_MS;
        return metaProfile.compareAndSetLease(meta.getId(), meta.getEpoch(), ownerNodeId, expireAt);
    }
}
