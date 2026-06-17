/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 通知类型枚举
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-05 00:27
 */
public enum NoticeTypeEnum {
    /**
     * 测试消息
     */
    TEST_MESSAGE(0),
    /**
     * 连接离线
     */
    CONNECTOR_OFFLINE(1),
    /**
     * 驱动异常
     */
    MAPPING_ERROR(2),
    /**
     * 驱动停止
     */
    MAPPING_STOP(3),
    /**
     * 通用消息
     */
    GENERAL_MESSAGE(4),
    /**
     * 订正校验失败
     */
    VALIDATE_SYNC_FAIL(5),
    /**
     * 系统消息
     */
    SYSTEM_MESSAGE(6);



    private final int code;

    NoticeTypeEnum(int code){
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
