/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.enums.SqlBuilderEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.util.List;
import java.util.stream.Collectors;

public interface Database {

    /**
     * 获取dbs唯一标识码
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
     * 获取主键字段名称
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

    /**
     * 获取查询总数SQL
     */
    default String getQueryCountSql(SqlBuilderConfig sqlConfig) {
        return SqlBuilderEnum.QUERY_COUNT.getSqlBuilder().buildSql(sqlConfig);
    }

    /**
     * 生成upsert
     */
    default String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        throw new SdkException("暂不支持开启upsert");
    }

    /**
     * 生成insert
     */
    default String buildInsertSql(SqlBuilderConfig config) {
        return SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(config);
    }

    default boolean buildCustom(List<String> fs, Field field) {
        return false;
    }

    /**
     * 为特殊字段类型构建自定义的值表达式
     * 
     * <p>用于 INSERT/UPDATE 语句的 VALUES 部分，允许数据库连接器为特定字段类型（如 geometry、geography）
     * 提供自定义的 SQL 表达式，而不是简单的占位符 ?</p>
     * 
     * <p>例如 SQL Server 的 geometry 类型需要使用 geometry::STGeomFromText(?, ?) 来转换</p>
     * 
     * @param vs 值表达式列表（VALUES 部分）
     * @param field 字段信息
     * @return true 表示已添加自定义值表达式，false 表示使用默认的 ? 占位符
     */
    default boolean buildCustomValue(List<String> vs, Field field) {
        return false;
    }
}