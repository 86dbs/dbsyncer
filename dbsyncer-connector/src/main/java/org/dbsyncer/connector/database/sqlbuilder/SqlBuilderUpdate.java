package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.Database;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderUpdate implements SqlBuilder {

    @Override
    public String buildSql(DatabaseConfig config, String tableName, String pk, List<String> filedNames, String queryFilter,
                           Database database) {
        StringBuilder sql = new StringBuilder();
        int size = filedNames.size();
        int end = size - 1;
        sql.append("UPDATE ").append(tableName).append(" SET ");
        for (int i = 0; i < size; i++) {
            // USER.USERNAME=?
            sql.append(tableName).append(".").append(filedNames.get(i)).append("=?");
            //如果不是最后一个字段
            if (i < end) {
                sql.append(",");
            }
        }
        // UPDATE USER SET USER.USERNAME=?,USER.AGE=?, WHERE USER.ID=?
        sql.append(" WHERE ").append(tableName).append(".").append(pk).append("=?");
        return sql.toString();
    }

}