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
     * 同步任务存在失败记录
     */
    MAPPING_ERROR(2),
    /**
     * 手动停止同步任务
     */
    MAPPING_STOP(3),
    /**
     * 订正校验失败
     */
    VALIDATE_SYNC_FAIL(4),
    /**
     * 授权异常
     */
    LICENSE_EXCEPTION(5),
    /**
     * 授权到期提醒
     */
    LICENSE_EXPIRE_REMIND(6),
    /**
     * 授权已过期
     */
    LICENSE_EXPIRED(7);

    private final int code;

    NoticeTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
