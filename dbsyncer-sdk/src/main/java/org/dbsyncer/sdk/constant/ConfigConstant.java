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

    /**
     * 表映射关系表(dbsyncer_table_group)拆分列
     */
    public static final String TABLE_GROUP_MAPPING_ID = "taskId";
    public static final String TABLE_GROUP_SORT_INDEX = "sortIndex";

    /**
     * 任务执行结果表(dbsyncer_meta)拆分列
     */
    public static final String META_STATE = "state";
    public static final String META_TOTAL = "total";
    public static final String META_SUCCESS = "metaSuccess";
    public static final String META_FAIL = "fail";

    /**
     * 任务执行明细表(dbsyncer_task_detail)精简列(每个任务一张分表, 表内单一类别)
     * <p>TYPE 列复用 {@link #CONFIG_MODEL_TYPE}: 同步数据存事件(insert/update/delete/DDL); 校验/迁移存子类型
     */
    public static final String DETAIL_IS_SUCCESS = "isSuccess";
    public static final String DETAIL_TARGET_TABLE = "targetTable";

    /**
     * 数据(同步明细分表列)
     */
    public static final String DATA_TABLE_GROUP_ID = "tableGroupId";
    public static final String DATA_TARGET_TABLE_NAME = "targetTableName";
    public static final String DATA_ERROR = "error";

    /**
     * Binlog
     */
    public static final String BINLOG_DATA = "data";

    /**
     * 任务
     */
    public static final String TASK_STATUS = "status";
    public static final String TASK_ID = "taskId";
    public static final String TASK_SOURCE_TABLE_NAME = "sourceTableName";
    public static final String TASK_SOURCE_TOTAL = "sourceTotal";
    public static final String TASK_TARGET_TOTAL = "targetTotal";
    public static final String TASK_DIFF_TOTAL = "diffTotal";
    public static final String TASK_FIXED_TOTAL = "fixedTotal";
    public static final String TASK_CONTENT = "content";

    /**
     * 整库迁移明细（与 {@link #TASK_SOURCE_TOTAL}、{@link #CONFIG_MODEL_CREATE_TIME} 等共用 camelCase 键）
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
}
