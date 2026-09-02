/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.storage.Strategy;
import org.dbsyncer.sdk.storage.strategy.ConfigStrategy;
import org.dbsyncer.sdk.storage.strategy.ConnectorStrategy;
import org.dbsyncer.sdk.storage.strategy.LogStrategy;
import org.dbsyncer.sdk.storage.strategy.MetaStrategy;
import org.dbsyncer.sdk.storage.strategy.TableGroupStrategy;
import org.dbsyncer.sdk.storage.strategy.TaskDetailStrategy;
import org.dbsyncer.sdk.storage.strategy.ClusterTaskStrategy;
import org.dbsyncer.sdk.storage.strategy.TaskStrategy;
import org.dbsyncer.sdk.storage.strategy.UserStrategy;
import org.dbsyncer.sdk.storage.strategy.ClusterNodeStrategy;

/**
 * 存储策略枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023-12-12 00:51
 */
public enum StorageStrategyEnum {

    /**
     * 配置策略
     */
    CONFIG(StorageEnum.CONFIG, new ConfigStrategy()),

    /**
     * 用户配置策略
     */
    USER(StorageEnum.USER, new UserStrategy()),

    /**
     * 连接配置策略
     */
    CONNECTOR(StorageEnum.CONNECTOR, new ConnectorStrategy()),

    /**
     * 表映射关系策略
     */
    TABLE_GROUP(StorageEnum.TABLE_GROUP, new TableGroupStrategy()),

    /**
     * 任务执行结果(meta)策略
     */
    META(StorageEnum.META, new MetaStrategy()),

    /**
     * 任务执行明细策略(统一 同步数据/订正校验/整库迁移 明细)
     */
    TASK_DETAIL(StorageEnum.TASK_DETAIL, new TaskDetailStrategy()),

    /**
     * 日志策略
     */
    LOG(StorageEnum.LOG, new LogStrategy()),

    /**
     * 任务策略
     */
    TASK(StorageEnum.TASK, new TaskStrategy()),

    /**
     * 集群节点
     */
    CLUSTER_NODE(StorageEnum.CLUSTER_NODE, new ClusterNodeStrategy()),

    /**
     * 集群任务调度
     */
    CLUSTER_TASK(StorageEnum.CLUSTER_TASK, new ClusterTaskStrategy());

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