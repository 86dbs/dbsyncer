package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/8/8 0:03
 */
public class SqlBuilderQueryExist extends SqlBuilderQuery {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        String tableName = config.getTableName();
        String schema = config.getSchema();
        List<String> primaryKeys = database.buildPrimaryKeys(config.getPrimaryKeys());
        StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ");
        sql.append(schema).append(quotation);
        sql.append(database.buildTableName(tableName));
        sql.append(quotation);
        sql.append(" WHERE ");
        // id = ? AND uid = ?
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " = ? ", true);
        return sql.toString();
    }

}