package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.sqlbuilder.SqlBuilder;

public abstract class AbstractSqlBuilder implements SqlBuilder {

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        throw new SdkException("Not implemented");
    }
}