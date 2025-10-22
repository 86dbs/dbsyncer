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
     * 删除
     */
    public static final String OPERTION_DELETE = "DELETE";

    /**
     * 插入或更新
     */
    public static final String OPERTION_UPSERT = "UPSERT";

    /**
     * 表结构更改
     */
    public static final String OPERTION_ALTER = "ALTER";

    /**
     * 查询
     */
    public static final String OPERTION_QUERY_STREAM = "QUERY_STREAM";

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
}