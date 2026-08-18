/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 集群节点存活/入群状态（与角色分离）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public enum ClusterNodeStatusEnum {

    JOINING(0),
    ONLINE(1),
    UNREACHABLE(2),
    OFFLINE(3),
    LEAVING(4);

    private final int code;

    ClusterNodeStatusEnum(int code) {
        this.code = code;
    }

    /**
     * 按编码解析，未知值视为 OFFLINE。
     *
     * @param code 编码
     * @return 状态
     */
    public static ClusterNodeStatusEnum fromCode(int code) {
        for (ClusterNodeStatusEnum e : values()) {
            if (e.code == code) {
                return e;
            }
        }
        return OFFLINE;
    }

    public int getCode() {
        return code;
    }
}
