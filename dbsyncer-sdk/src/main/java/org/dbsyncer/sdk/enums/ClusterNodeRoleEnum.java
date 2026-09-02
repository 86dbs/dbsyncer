/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 集群节点角色（对应 {@code dbsyncer_cluster_node.ROLE}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-02
 */
public enum ClusterNodeRoleEnum {

    FOLLOWER(0, "Follower"),
    LEADER(1, "Leader");

    private final int code;
    private final String message;

    ClusterNodeRoleEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static ClusterNodeRoleEnum fromCode(int code) {
        for (ClusterNodeRoleEnum item : values()) {
            if (item.code == code) {
                return item;
            }
        }
        return FOLLOWER;
    }
}
