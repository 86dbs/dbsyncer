/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019-11-12 20:29
 */
public enum StorageEnum {

    /**
     * 配置：连接器、驱动、映射关系、同步信息、系统配置、用戶配置
     */
    CONFIG("config"),
    /**
     * 日志：连接器、驱动、映射关系、同步信息、系统日志
     */
    LOG("log"),
    /**
     * 数据：全量或增量数据
     */
    DATA("data"),
    /**
     * 任务
     */
    TASK("task"),
    /**
     * 订正校验明细
     */
    VALIDATE_SYNC_DETAIL("task_validata_sync_detail");

    private final String type;

    StorageEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
