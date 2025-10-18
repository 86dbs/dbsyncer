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
    public static final String PROJECT_GROUP = "projectGroup";

    /**
     * 数据
     */
    public static final String DATA_SUCCESS = "success";
    public static final String DATA_TABLE_GROUP_ID = "tableGroupId";
    public static final String DATA_TARGET_TABLE_NAME = "targetTableName";
    public static final String DATA_EVENT = "event";
    public static final String DATA_ERROR = "error";
    /**
     * Binlog
     */
    public static final String BINLOG_DATA = "data";

    /**
     * 任务
     */
    public static final String TASK_STATUS = "status";
    public static final String TASK_ID = "task_id";
    public static final String TASK_SOURCE_TABLE_NAME = "source_table_name";
    public static final String TASK_TARGET_TABLE_NAME = "target_table_name";
    public static final String TASK_CONTENT = "content";

}