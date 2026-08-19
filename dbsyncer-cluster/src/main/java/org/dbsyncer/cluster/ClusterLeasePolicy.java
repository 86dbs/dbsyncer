/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.cluster;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Meta;

/**
 * 表租约分配判定，与 Raft/存储解耦便于单测。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class ClusterLeasePolicy {

    private ClusterLeasePolicy() {
    }

    /**
     * 表是否分配给本节点且租约未过期。
     *
     * @param detail      表级 Meta
     * @param localNodeId 本节点 ID
     * @param now         当前时间
     * @return true 本节点应执行
     */
    public static boolean assignedToLocal(Meta detail, String localNodeId, long now) {
        if (detail == null || StringUtil.isBlank(localNodeId)) {
            return false;
        }
        return StringUtil.equals(localNodeId, detail.getLeaseOwner()) && detail.getLeaseExpireAt() > now;
    }

    /**
     * 租约是否空闲或已过期。
     *
     * @param meta 任务级或表级 Meta
     * @param now  当前时间
     * @return true 可改派
     */
    public static boolean leaseFree(Meta meta, long now) {
        if (meta == null) {
            return true;
        }
        return StringUtil.isBlank(meta.getLeaseOwner()) || meta.getLeaseExpireAt() <= now;
    }

    /**
     * 表是否已完成（表级 STATE 或任务级 tableProgress）。
     *
     * @param detail       表级 Meta
     * @param progressDone 任务级进度已 done
     * @return true 跳过分配
     */
    public static boolean tableDone(Meta detail, boolean progressDone) {
        if (progressDone) {
            return true;
        }
        return detail != null && detail.getState() == CommonTaskStatusEnum.DONE.getCode();
    }

    /**
     * 未过期租约且 owner 仍在线时续租；owner 掉线但未过期则等待，不抢。
     *
     * @param meta         表级 Meta
     * @param ownerOnline  owner 是否 ONLINE
     * @param now          当前时间
     * @return true 应续租
     */
    public static boolean shouldRenew(Meta meta, boolean ownerOnline, long now) {
        return meta != null && !leaseFree(meta, now) && ownerOnline;
    }

    /**
     * 无主或已过期才改派。
     *
     * @param meta 表级 Meta
     * @param now  当前时间
     * @return true 应改派
     */
    public static boolean shouldReassign(Meta meta, long now) {
        return leaseFree(meta, now);
    }
}
