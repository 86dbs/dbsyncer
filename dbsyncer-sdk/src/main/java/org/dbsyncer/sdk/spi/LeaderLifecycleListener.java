/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * Leader 升降回调（控制面）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface LeaderLifecycleListener {

    /**
     * 本节点成为 Leader。
     *
     * @param term 选举任期
     */
    void onLeaderStart(long term);

    /**
     * 本节点不再是 Leader。
     *
     * @param term 卸任时的任期
     */
    void onLeaderStop(long term);
}
