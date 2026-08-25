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
    /**
     * 连接器可作为源端（拆分列 IS_SOURCE，1-是 0-否）
     */
    public static final String CONNECTOR_IS_SOURCE = "isSource";
    /**
     * 连接器可作为目标端（拆分列 IS_TARGET，1-是 0-否）
     */
    public static final String CONNECTOR_IS_TARGET = "isTarget";
    public static final String MAPPING = "mapping";
    public static final String TABLE_GROUP = "tableGroup";
    public static final String META = "meta";
    public static final String TASK = "task";
    /**
     * 企业任务 type（存 {@code dbsyncer_task}），与 {@code CommonTaskTypeEnum#name()} 一致
     */
    public static final String VALIDATE_SYNC = "VALIDATE_SYNC";
    public static final String DATABASE_SYNC = "DATABASE_SYNC";

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
    public static final String USER_EMAIL = "email";
    public static final String USER_PHONE = "phone";

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
    /**
     * 任务开始时间（列 START_TIME），对应 Meta.startTime
     */
    public static final String META_START_TIME = "startTime";
    public static final String META_STATE = "state";
    public static final String META_IS_TASK_DETAIL = "isTaskDetail";
    public static final String META_TOTAL = "total";
    public static final String META_SUCCESS = "success";
    public static final String META_FAIL = "fail";
    public static final String META_DIFF = "diff";
    public static final String META_FIXED = "fixed";
    public static final String META_SNAPSHOT = "snapshot";

    /**
     * 集群节点表(dbsyncer_cluster_node)
     */
    public static final String CLUSTER_NODE = "cluster_node";
    public static final String CLUSTER_CLUSTER_ID = "clusterId";
    public static final String CLUSTER_NODE_ID = "nodeId";
    public static final String CLUSTER_IP = "ip";
    public static final String CLUSTER_HTTP_PORT = "httpPort";
    public static final String CLUSTER_RAFT_PORT = "raftPort";
    public static final String CLUSTER_RAFT_PEER_ID = "raftPeerId";
    public static final String CLUSTER_WORKER_ID = "workerId";
    public static final String CLUSTER_ROLE = "role";
    public static final String CLUSTER_STATUS = "status";
    public static final String CLUSTER_NETWORK_OK = "networkOk";
    public static final String CLUSTER_TERM = "term";
    public static final String CLUSTER_LAST_HEARTBEAT_TIME = "lastHeartbeatTime";
    public static final String CLUSTER_START_TIME = "startTime";

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
    public static final int PAGE_SIZE= 1000;

}
