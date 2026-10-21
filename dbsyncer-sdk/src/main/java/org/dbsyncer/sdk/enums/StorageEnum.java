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
     * 全局配置：收窄为系统配置(system)/通知配置(notice)
     */
    CONFIG("config"),
    /**
     * 用户配置
     */
    USER("user"),
    /**
     * 连接配置
     */
    CONNECTOR("connector"),
    /**
     * 表映射关系配置
     */
    TABLE_GROUP("table_group"),
    /**
     * 任务执行结果(meta)
     */
    META("meta"),
    /**
     * 日志：连接器、驱动、映射关系、同步信息、系统日志
     */
    LOG("log"),
    /**
     * 任务
     */
    TASK("task"),
    /**
     * 任务执行明细(统一 同步数据/订正校验/整库迁移 明细)
     */
    TASK_DETAIL("task_detail"),
    /**
     * 集群节点
     */
    CLUSTER_NODE("cluster_node"),
    /**
     * 集群任务调度
     */
    CLUSTER_TASK("cluster_task"),
    /**
     * 任务分片计划
     */
    TASK_PLAN("cluster_task_plan"),
    /**
     * 业务分组
     */
    GROUP("group"),
    /**
     * 业务分组关系
     */
    GROUP_REL("group_rel"),
    /**
     * 角色
     */
    ROLE("role"),
    /**
     * 权限点
     */
    PERMISSION("permission"),
    /**
     * 角色-权限
     */
    ROLE_PERMISSION("role_permission"),
    /**
     * 用户-角色
     */
    USER_ROLE("user_role"),
    /**
     * 资源访问控制
     */
    RESOURCE_ACL("resource_acl");

    private final String type;

    StorageEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
