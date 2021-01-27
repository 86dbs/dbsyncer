package org.dbsyncer.connector.database;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.sqlbuilder.SqlBuilder;

public abstract class AbstractSqlBuilder implements SqlBuilder {

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        throw new ConnectorException("Not implemented");
    }
}