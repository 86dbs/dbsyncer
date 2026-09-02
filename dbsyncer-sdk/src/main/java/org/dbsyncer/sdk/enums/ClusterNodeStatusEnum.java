/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 集群节点在线状态。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public enum ClusterNodeStatusEnum {

    OFFLINE(0),
    ONLINE(1);

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
