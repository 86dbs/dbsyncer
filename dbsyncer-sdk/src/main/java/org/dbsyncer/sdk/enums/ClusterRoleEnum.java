/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 集群节点 Raft 角色（表投影，权威在 Raft）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public enum ClusterRoleEnum {

    FOLLOWER(0),
    LEADER(1),
    LEARNER(2);

    private final int code;

    ClusterRoleEnum(int code) {
        this.code = code;
    }

    /**
     * 按编码解析，未知值视为 FOLLOWER。
     *
     * @param code 编码
     * @return 角色
     */
    public static ClusterRoleEnum fromCode(int code) {
        for (ClusterRoleEnum e : values()) {
            if (e.code == code) {
                return e;
            }
        }
        return FOLLOWER;
    }

    public int getCode() {
        return code;
    }
}
