package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderDelete extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        String tableName = config.getTableName();
        List<String> primaryKeys = database.buildPrimaryKeys(config.getPrimaryKeys());
        // DELETE FROM "USER" WHERE "ID"=? AND "UID" = ?
        StringBuilder sql = new StringBuilder().append("DELETE FROM ").append(config.getSchema());
        sql.append(quotation);
        sql.append(database.buildTableName(tableName));
        sql.append(quotation);
        sql.append(" WHERE ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " = ? ", true);
        return sql.toString();
    }

}