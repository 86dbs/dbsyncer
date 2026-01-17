package org.dbsyncer.sdk.constant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:56
 */
public class ConnectorConstant {

    /**
     * 新增
     */
    public static final String OPERTION_INSERT = "INSERT";

    /**
     * 更新
     */
    public static final String OPERTION_UPDATE = "UPDATE";

    /**
     * 覆盖更新
     */
    public static final String OPERTION_UPSERT = "UPSERT";

    /**
     * 删除
     */
    public static final String OPERTION_DELETE = "DELETE";

    /**
     * 表结构更改
     */
    public static final String OPERTION_ALTER = "ALTER";

    /**
     * 查询
     */
    public static final String OPERTION_QUERY = "QUERY";

    /**
     * 查询游标
     */
    public static final String OPERTION_QUERY_CURSOR = "QUERY_CURSOR";

    /**
     * 查询过滤条件
     */
    public static final String OPERTION_QUERY_FILTER = "QUERY_FILTER";

    /**
     * 查询总数
     */
    public static final String OPERTION_QUERY_COUNT = "QUERY_COUNT";

    /**
     * 主表，扩展表映射关系
     * <p>场景1:支持自定义SQL作为新表，需要根据主表监听增量数据
     * <p>场景2:支持自定义半结构化字段作为新表，需要根据Topic（Kafka），FileName（File）监听增量数据
     */
    public static final String CUSTOM_TABLE_MAIN = "CT_MAIN";
    public static final String CUSTOM_TABLE_SQL = "CT_SQL";

}