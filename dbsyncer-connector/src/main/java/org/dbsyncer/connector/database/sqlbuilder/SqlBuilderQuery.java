package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;
import org.dbsyncer.connector.database.Database;

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
        Database database = config.getDatabase();
        PageSql pageSql = new PageSql(config, buildQuerySql(config), config.getQuotation(), config.getPrimaryKeys());
        return database.getPageSql(pageSql);
    }

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        String tableName = config.getTableName();
        List<Field> fields = config.getFields();
        String quotation = config.getQuotation();
        String queryFilter = config.getQueryFilter();

        StringBuilder sql = new StringBuilder();
        int size = fields.size();
        int end = size - 1;
        Field field = null;
        for (int i = 0; i < size; i++) {
            field = fields.get(i);
            if (field.isUnmodifiabled()) {
                sql.append(field.getName());
            } else {
                sql.append(quotation).append(field.getName()).append(quotation);
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
        sql.insert(0, "SELECT ").append(" FROM ").append(config.getSchema()).append(quotation).append(tableName).append(quotation);
        // 解析查询条件
        if (StringUtil.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        return sql.toString();
    }

}