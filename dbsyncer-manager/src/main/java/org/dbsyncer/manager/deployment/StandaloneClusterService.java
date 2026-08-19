/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.parser.MetaProfile;
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
 * 单机控制面：恒为 Leader。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneClusterService implements ClusterService {

    public static final String NODE_ID = "standalone";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<LeaderLifecycleListener> listeners = new CopyOnWriteArrayList<>();

    public StandaloneClusterService(MetaProfile metaProfile) {
        // metaProfile 保留构造参数以兼容既有注入，单机派工不再依赖租约列
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
}
