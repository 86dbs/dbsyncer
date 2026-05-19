/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.storage.Strategy;
import org.dbsyncer.sdk.storage.strategy.ConfigStrategy;
import org.dbsyncer.sdk.storage.strategy.DataStrategy;
import org.dbsyncer.sdk.storage.strategy.LogStrategy;
import org.dbsyncer.sdk.storage.strategy.TaskDataVerificationDetailStrategy;
import org.dbsyncer.sdk.storage.strategy.TaskStrategy;

/**
 * 存储策略枚举
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-12 00:51
 */
public enum StorageStrategyEnum {

    /**
     * 配置策略
     */
    CONFIG(StorageEnum.CONFIG, new ConfigStrategy()),

    /**
     * 数据策略
     */
    DATA(StorageEnum.DATA, new DataStrategy()),

    /**
     * 日志策略
     */
    LOG(StorageEnum.LOG, new LogStrategy()),

    /**
     * 任务策略
     */
    TASK(StorageEnum.TASK, new TaskStrategy()),

    /**
     * 数据校验明细策略
     */
    TASK_DATA_VERIFICATION_DETAIL(StorageEnum.TASK_DATA_VERIFICATION_DETAIL, new TaskDataVerificationDetailStrategy());

    private final StorageEnum type;
    private final Strategy strategy;

    StorageStrategyEnum(StorageEnum type, Strategy strategy) {
        this.type = type;
        this.strategy = strategy;
    }

    public static Strategy getStrategy(StorageEnum type) throws SdkException {
        for (StorageStrategyEnum e : StorageStrategyEnum.values()) {
            if (type == e.getType()) {
                return e.getStrategy();
            }
        }
        throw new SdkException(String.format("StorageStrategy type \"%s\" does not exist.", type));
    }

    public StorageEnum getType() {
        return type;
    }

    public Strategy getStrategy() {
        return strategy;
    }
}