package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderQuery extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        // 分页语句
        List<String> primaryKeys = config.getPrimaryKeys();
        String queryFilter = config.getQueryFilter();
        List<Field> fields = config.getFields();
        PageSql pageSql = new PageSql(buildQuerySql(config), queryFilter, primaryKeys, fields);
        return config.getDatabase().getPageSql(pageSql);
    }

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        List<Field> fields = config.getFields();
        String queryFilter = config.getQueryFilter();

        StringBuilder sql = new StringBuilder("SELECT ");
        int size = fields.size();
        int end = size - 1;
        Field field = null;
        for (int i = 0; i < size; i++) {
            field = fields.get(i);
            if (field.isUnmodifiabled()) {
                sql.append(field.getName());
            } else {
                sql.append(quotation);
                sql.append(database.buildFieldName(field));
                sql.append(quotation);
            }

            // "USERNAME" as "myName"
            if (StringUtil.isNotBlank(field.getLabelName())) {
                sql.append(" as ").append(quotation).append(field.getLabelName()).append(quotation);
            }

            //如果不是最后一个字段
            if (i < end) {
                sql.append(", ");
            }
        }
        // SELECT "ID","NAME" FROM "USER"
        sql.append(" FROM ").append(config.getSchema()).append(quotation);
        sql.append(database.buildTableName(config.getTableName()));
        sql.append(quotation);
        // 解析查询条件
        if (StringUtil.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        return sql.toString();
    }

}