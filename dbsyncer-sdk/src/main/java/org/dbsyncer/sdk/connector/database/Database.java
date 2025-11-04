/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.util.List;

public interface Database {

    /**
     * 获取表名称(可自定义处理系统关键字，函数名)
     *
     * @param tableName
     * @return
     */
    default String buildTableName(String tableName) {
        return tableName;
    }

    /**
     * 获取字段名称(可自定义处理系统关键字，函数名)
     *
     * @param field
     * @return
     */
    default String buildFieldName(Field field) {
        return field.getName();
    }

    /**
     * 获取主键字段名称(可自定义处理系统关键字，函数名)
     *
     * @param primaryKeys
     * @return
     */
    default List<String> buildPrimaryKeys(List<String> primaryKeys) {
        return primaryKeys;
    }


    /**
     * 获取流式处理的 fetchSize 值
     *
     * @param context 读取上下文
     * @return fetchSize 值
     */
    Integer getStreamingFetchSize(ReaderContext context);


    /**
     * 健康检查
     *
     * @return
     */
    default String getValidationQuery() {
        return "select 1";
    }
}