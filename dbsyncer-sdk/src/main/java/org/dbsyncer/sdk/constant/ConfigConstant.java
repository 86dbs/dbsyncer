/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.constant;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-16 22:14
 */
public class ConfigConstant {

    /**
     * 公共属性
     */
    public static final String CONFIG_MODEL_ID = "id";
    public static final String CONFIG_MODEL_NAME = "name";
    public static final String CONFIG_MODEL_TYPE = "type";
    public static final String CONFIG_MODEL_CREATE_TIME = "createTime";
    public static final String CONFIG_MODEL_UPDATE_TIME = "updateTime";
    public static final String CONFIG_MODEL_JSON = "json";

    /**
     * 配置类型
     */
    public static final String SYSTEM = "system";
    public static final String USER = "user";
    public static final String CONNECTOR = "connector";
    public static final String MAPPING = "mapping";
    public static final String TABLE_GROUP = "tableGroup";
    public static final String META = "meta";
    public static final String TASK = "task";
    public static final String GROUP = "group";
    public static final String GROUP_REL = "groupRel";
    public static final String ROLE = "role";
    public static final String PERMISSION = "permission";
    public static final String ROLE_PERMISSION = "rolePermission";
    /**
     * 用户-角色关系配置类型（勿与用户表列 {@link #USER_ROLE} 混淆）
     */
    public static final String USER_ROLE_REL = "userRole";
    /**
     * 资源访问控制
     */
    public static final String RESOURCE_ACL = "resourceAcl";
    /**
     * 告警通知规则
     */
    public static final String NOTICE_RULE = "noticeRule";
    /**
     * 告警策略
     */
    public static final String NOTICE_STRATEGY = "noticeStrategy";

    /**
     * 连接器配置字段
     */
    public static final String CONNECTOR_IS_SOURCE = "isSource";
    public static final String CONNECTOR_IS_TARGET = "isTarget";
    public static final String CONNECTOR_STATUS = "status";

    /**
     * 企业任务 type（存 {@code dbsyncer_task}），与 {@code CommonTaskTypeEnum#name()} 一致
     */
    public static final String VALIDATE_SYNC = "validateSync";
    public static final String DATABASE_SYNC = "databaseSync";

    /**
     * 业务分组关系表(dbsyncer_group_rel)拆分列
     */
    public static final String GROUP_REL_GROUP_ID = "groupId";
    /**
     * 资源类型列 RESOURCE_TYPE（模型侧复用 {@link #CONFIG_MODEL_TYPE}/{@code ConfigModel#type}）
     */
    public static final String GROUP_REL_RESOURCE_TYPE = "resourceType";
    public static final String GROUP_REL_RESOURCE_ID = "resourceId";

    /**
     * 菜单权限表(dbsyncer_permission)拆分列
     */
    public static final String PERMISSION_CODE = "code";
    public static final String PERMISSION_MODULE = "module";

    /**
     * 角色权限关系表(dbsyncer_role_permission)拆分列
     */
    public static final String ROLE_PERMISSION_ROLE_ID = "roleId";
    public static final String ROLE_PERMISSION_PERMISSION_ID = "permissionId";

    /**
     * 用户角色关系表(dbsyncer_user_role)拆分列
     */
    public static final String USER_ROLE_USER_ID = "userId";
    public static final String USER_ROLE_ROLE_ID = "roleId";

    /**
     * 资源 ACL 表(dbsyncer_resource_acl)拆分列
     */
    public static final String RESOURCE_ACL_SUBJECT_TYPE = "subjectType";
    public static final String RESOURCE_ACL_SUBJECT_ID = "subjectId";
    public static final String RESOURCE_ACL_PERMISSION = "permission";

    /**
     * 告警通知规则表(dbsyncer_notice_rule)拆分列
     */
    public static final String NOTICE_RULE_SCOPE_TYPE = "scopeType";
    public static final String NOTICE_RULE_SCOPE_ID = "scopeId";
    public static final String NOTICE_RULE_AT_ALL = "atAll";
    public static final String NOTICE_RULE_AT_PHONES = "atPhones";
    public static final String NOTICE_RULE_EMAILS = "emails";

    /**
     * 告警策略表(dbsyncer_notice_strategy)拆分列
     */
    public static final String NOTICE_STRATEGY_ENABLED = "enabled";
    public static final String NOTICE_STRATEGY_FREQUENCY = "frequency";
    public static final String NOTICE_STRATEGY_CRON = "cron";
    public static final String NOTICE_STRATEGY_NOTICE_TYPES = "noticeTypes";
    public static final String NOTICE_STRATEGY_REMARK = "remark";

    /**
     * 日志表(dbsyncer_log)筛选列
     */
    public static final String LOG_USERNAME = "username";
    public static final String LOG_IP = "ip";
    public static final String LOG_STATUS = "status";

    /**
     * 用户表(dbsyncer_user)拆分列
     */
    public static final String USER_USERNAME = "username";
    public static final String USER_PASSWORD = "password";
    public static final String USER_NICKNAME = "nickname";
    /**
     * 列名 ROLE，模型字段 roleCode
     */
    public static final String USER_ROLE = "role";
    /**
     * 连续登录失败次数
     */
    public static final String USER_LOGIN_FAIL_COUNT = "loginFailCount";
    /**
     * 锁定截止时间（毫秒时间戳，0 表示未锁定）
     */
    public static final String USER_LOCK_UNTIL = "lockUntil";

    /**
     * 表映射关系表(dbsyncer_table_group)拆分列
     */
    public static final String TABLE_GROUP_TASK_ID = "taskId";
    public static final String TABLE_GROUP_SORT_INDEX = "sortIndex";
    public static final String TABLE_GROUP_SOURCE_CONNECTOR_ID = "sourceConnectorId";
    public static final String TABLE_GROUP_TARGET_CONNECTOR_ID = "targetConnectorId";
    public static final String TABLE_GROUP_SOURCE_DATABASE = "sourceDatabase";
    public static final String TABLE_GROUP_TARGET_DATABASE = "targetDatabase";
    public static final String TABLE_GROUP_SOURCE_SCHEMA = "sourceSchema";
    public static final String TABLE_GROUP_TARGET_SCHEMA = "targetSchema";
    public static final String TABLE_GROUP_SOURCE_TABLE = "sourceTable";
    public static final String TABLE_GROUP_TARGET_TABLE = "targetTable";
    public static final String TABLE_GROUP_SOURCE_TOTAL = "sourceTotal";
    public static final String TABLE_GROUP_TARGET_TOTAL = "targetTotal";

    /**
     * 任务执行结果表(dbsyncer_meta)拆分列
     */
    public static final String META_TASK_ID = "taskId";
    public static final String META_STATE = "state";
    public static final String META_IS_TASK_DETAIL = "isTaskDetail";
    public static final String META_TOTAL = "total";
    public static final String META_SUCCESS = "success";
    public static final String META_FAIL = "fail";
    public static final String META_DIFF = "diff";
    public static final String META_FIXED = "fixed";
    public static final String META_SNAPSHOT = "snapshot";
    /**
     * 任务启动时间（落库列 START_TIME）
     */
    public static final String META_START_TIME = "startTime";

    /**
     * 集群节点表(dbsyncer_cluster_node)
     */
    public static final String CLUSTER_NODE_ID = "nodeId";
    public static final String CLUSTER_IP = "ip";
    public static final String CLUSTER_HTTP_PORT = "httpPort";
    public static final String CLUSTER_WORKER_ID = "workerId";
    public static final String CLUSTER_STATUS = "status";
    public static final String CLUSTER_ROLE = "role";
    public static final String CLUSTER_TERM = "term";
    public static final String CLUSTER_HEARTBEAT_TIME = "heartbeatTime";
    public static final String CLUSTER_START_TIME = "startTime";

    /**
     * 集群任务调度表(dbsyncer_cluster_task)
     */
    public static final String CLUSTER_TASK_TABLE = "dbsyncer_cluster_task";
    public static final String SCHEDULE_NODE_ID = "nodeId";
    public static final String SCHEDULE_TASK_TYPE = "taskType";

    /**
     * 任务分片计划表(dbsyncer_cluster_task_plan)
     */
    public static final String CLUSTER_TASK_PLAN_TABLE = "dbsyncer_cluster_task_plan";
    public static final String PLAN_START_CURSOR = "startCursor";
    public static final String PLAN_END_CURSOR = "endCursor";
    public static final String PLAN_LAST_CURSOR = "lastCursor";
    public static final String PLAN_SUCCESS = "success";
    public static final String PLAN_FAIL = "fail";
    public static final String PLAN_LAST_PAGE = "lastPage";
    public static final String PLAN_STATUS = "status";

    /**
     * 任务级 Meta.SNAPSHOT 内键：整库迁移库映射 status 摘要 JSON（不含表级 tables）
     */
    public static final String META_SNAPSHOT_DATABASE = "databaseSnapshots";

    /**
     * 结果 Meta.SNAPSHOT 内键：单表运行快照 JSON（TASK_ID=detail.id）
     */
    public static final String META_SNAPSHOT_TABLE_ONE = "tableSnapshot";

    /**
     * 结果 Meta.SNAPSHOT 内键：整库迁移时该表所属库映射 index
     */
    public static final String META_SNAPSHOT_MAPPING_INDEX = "mappingIndex";

    /**
     * 任务执行明细表(dbsyncer_task_detail)精简列(按任务分表)
     * <p>TYPE 列复用 {@link #CONFIG_MODEL_TYPE}
     */
    public static final String DETAIL_IS_SUCCESS = "isSuccess";
    public static final String DETAIL_TARGET_TABLE = "targetTable";
    public static final String DATA_TABLE_GROUP_ID = "tableGroupId";
    public static final String DATA_ERROR = "error";

    /**
     * Binlog / 明细载荷
     */
    public static final String BINLOG_DATA = "data";

    /**
     * 任务/前端兼容键
     */
    public static final String TASK_STATUS = "status";
    public static final String TASK_ID = "taskId";
    public static final String DATA_TARGET_TABLE_NAME = "targetTableName";
    public static final String TASK_SOURCE_TABLE_NAME = "sourceTableName";
    public static final String TASK_SOURCE_TOTAL = "sourceTotal";
    public static final String TASK_TARGET_TOTAL = "targetTotal";
    public static final String TASK_DIFF_TOTAL = "diffTotal";
    public static final String TASK_FIXED_TOTAL = "fixedTotal";
    public static final String TASK_CONTENT = "content";

    /**
     * 整库迁移展示字段(连表/VO 映射键，非独立明细列)
     */
    public static final String DATABASE_SYNC_DETAIL_TABLE_INDEX = "tableIndex";
    public static final String DATABASE_SYNC_DETAIL_SOURCE_DATABASE = "sourceDatabase";
    public static final String DATABASE_SYNC_DETAIL_SOURCE_SCHEMA = "sourceSchema";
    public static final String DATABASE_SYNC_DETAIL_TARGET_DATABASE = "targetDatabase";
    public static final String DATABASE_SYNC_DETAIL_TARGET_SCHEMA = "targetSchema";
    public static final String DATABASE_SYNC_DETAIL_SOURCE_TABLE = "sourceTable";
    public static final String DATABASE_SYNC_DETAIL_TARGET_TABLE = "targetTable";
    public static final String DATABASE_SYNC_DETAIL_SUCCESS_TOTAL = "successTotal";
    public static final String DATABASE_SYNC_DETAIL_FAIL_TOTAL = "failTotal";

    /**
     * 内部分页加载默认分页大小
     */
    public static final int PAGE_SIZE = 1000;

}
