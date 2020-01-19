package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.Database;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderDelete implements SqlBuilder {

    @Override
    public String buildSql(DatabaseConfig config, String tableName, String pk, List<String> filedNames, String queryFilter,
                           Database database) {
        // DELETE FROM USER WHERE USER.ID=?
        return new StringBuilder().append("DELETE FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(pk)
                .append("=?").toString();
    }

}