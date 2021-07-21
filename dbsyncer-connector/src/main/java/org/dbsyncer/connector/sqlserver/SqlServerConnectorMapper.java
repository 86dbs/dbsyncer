package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.HandleCallback;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.database.DatabaseTemplate;

import java.sql.Connection;

public final class SqlServerConnectorMapper extends ConnectorMapper {

    public SqlServerConnectorMapper(ConnectorConfig config, Connection connection) {
        super(config, connection);
    }

    @Override
    public <T> T execute(HandleCallback callback) {
        try {
            return (T) callback.apply(new DatabaseTemplate(connection));
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }
}
