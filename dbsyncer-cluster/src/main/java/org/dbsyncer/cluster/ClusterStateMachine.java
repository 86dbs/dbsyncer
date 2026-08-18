/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.cluster;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import org.dbsyncer.sdk.spi.LeaderLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 仅用于选主的空状态机（控制面，不复制业务数据）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public class ClusterStateMachine extends StateMachineAdapter {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<LeaderLifecycleListener> listeners = new CopyOnWriteArrayList<>();
    private volatile long leaderTerm = -1L;
    private volatile Configuration committedConf = new Configuration();

    /**
     * 注册升降主监听。
     *
     * @param listener 监听器
     */
    public void addListener(LeaderLifecycleListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    /**
     * 当前是否 Leader。
     *
     * @return true Leader
     */
    public boolean isLeader() {
        return leaderTerm > 0;
    }

    /**
     * 当前 term。
     *
     * @return term
     */
    public long getLeaderTerm() {
        return leaderTerm;
    }

    /**
     * 已提交的 Raft 成员配置，Follower 也可读。
     *
     * @return 配置副本
     */
    public Configuration getCommittedConf() {
        Configuration conf = this.committedConf;
        return conf == null ? new Configuration() : conf.copy();
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        this.committedConf = conf == null ? new Configuration() : conf.copy();
        logger.info("Raft 配置已提交: {}", this.committedConf);
    }

    @Override
    public void onApply(Iterator iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    @Override
    public void onLeaderStart(long term) {
        this.leaderTerm = term;
        logger.info("成为 Leader, term={}", term);
        for (LeaderLifecycleListener listener : listeners) {
            try {
                listener.onLeaderStart(term);
            } catch (Exception e) {
                logger.error("onLeaderStart 回调失败: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public void onLeaderStop(Status status) {
        long term = this.leaderTerm;
        this.leaderTerm = -1L;
        logger.info("卸任 Leader, term={}, status={}", term, status);
        for (LeaderLifecycleListener listener : listeners) {
            try {
                listener.onLeaderStop(term);
            } catch (Exception e) {
                logger.error("onLeaderStop 回调失败: {}", e.getMessage(), e);
            }
        }
    }
}
