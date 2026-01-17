/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.Database;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/8/8 0:03
 */
public class SqlBuilderQueryCount extends SqlBuilderQuery {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String queryFilter = config.getQueryFilter();
        return String.format("SELECT COUNT(*) FROM %s%s %s",
                config.getSchema(),
                database.buildWithQuotation(config.getTableName()),
                queryFilter);
    }

}