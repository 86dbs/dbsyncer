/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderUpdate extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();

        StringBuilder sql = new StringBuilder(database.generateUniqueCode());
        sql.append("UPDATE ").append(config.getSchema());
        sql.append(database.buildWithQuotation(config.getTableName()));
        sql.append(" SET ");

        List<String> fs = new ArrayList<>();
        for (Field f : config.getFields()) {
            fs.add(database.buildWithQuotation(f.getName()) + "=?");
        }
        sql.append(StringUtil.join(fs, StringUtil.COMMA));

        // UPDATE "USER" SET "USERNAME"=?,"AGE"=? WHERE "ID"=? AND "UID" = ?
        sql.append(" WHERE ");
        database.appendPrimaryKeys(sql, config.getPrimaryKeys());
        return sql.toString();
    }

}