/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.util.List;
import java.util.stream.Collectors;

public interface Database {

    /**
     * 获取dbs唯一标识码
     *
     * @return
     */
    default String generateUniqueCode() {
        return StringUtil.EMPTY;
    }

    /**
     * 获取引号（默认不加）
     */
    default String buildSqlWithQuotation() {
        return StringUtil.EMPTY;
    }

    /**
     * 返回带引号的名称
     */
    default String buildWithQuotation(String name) {
        return buildSqlWithQuotation() + name + buildSqlWithQuotation();
    }

    /**
     * 获取主键字段名称(可自定义处理系统关键字，函数名)
     *
     * @param primaryKeys
     * @return
     */
    default List<String> buildPrimaryKeys(List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return primaryKeys;
        }
        return primaryKeys.stream().map(this::buildWithQuotation).collect(Collectors.toList());
    }

    /**
     * 追加主键和参数占位符
     *
     * @param sql
     * @param primaryKeys
     */
    default void appendPrimaryKeys(StringBuilder sql, List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return;
        }
        List<String> pks = primaryKeys.stream().map(name -> buildWithQuotation(name) + "=?").collect(Collectors.toList());
        sql.append(StringUtil.join(pks, " AND "));
    }

    /**
     * 获取分页SQL
     *
     * @param config
     * @return
     */
    String getPageSql(PageSql config);

    /**
     * 获取分页参数
     *
     * @param context
     * @return
     */
    Object[] getPageArgs(ReaderContext context);

    /**
     * 获取游标分页SQL
     *
     * @param pageSql
     * @return
     */
    String getPageCursorSql(PageSql pageSql);

    /**
     * 获取游标分页参数
     *
     * @param context
     * @return
     */
    Object[] getPageCursorArgs(ReaderContext context);

    /**
     * 健康检查
     *
     * @return
     */
    default String getValidationQuery() {
        return "select 1";
    }

}