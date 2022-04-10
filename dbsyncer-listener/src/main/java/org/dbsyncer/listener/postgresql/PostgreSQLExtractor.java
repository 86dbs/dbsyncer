package org.dbsyncer.listener.postgresql;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/10 22:36
 */
public class PostgreSQLExtractor extends AbstractExtractor {

    private static final String GET_VALIDATION = "SELECT 1";
    private static final String GET_ROLE = "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin FROM pg_roles r WHERE r.rolname = current_user";
    private static final String GET_WAL_LEVEL = "SHOW WAL_LEVEL";
    private static final String DEFAULT_WAL_LEVEL = "logical";
    private static final String DEFAULT_SLOT_NAME = "DBSYNCER_SLOT";
    private static final String DEFAULT_PLUGIN_NAME = "wal2json";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private Connection connection;
    private PGReplicationStream stream;
    private DatabaseConfig config;
    private DatabaseConnectorMapper connectorMapper;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("PostgreSQLExtractor is already started");
                return;
            }

            connect();

            connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_VALIDATION, Integer.class));
            logger.info("Successfully tested connection for {} with user '{}'", config.getUrl(), config.getUsername());

            final String walLevel = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_WAL_LEVEL, String.class));
            if (!DEFAULT_WAL_LEVEL.equals(walLevel)) {
                throw new ListenerException(String.format("Postgres server wal_level property must be \"%s\" but is: %s", DEFAULT_WAL_LEVEL, walLevel));
            }

            final boolean hasAuth = connectorMapper.execute(databaseTemplate -> {
                Map rs = databaseTemplate.queryForObject(GET_ROLE, Map.class);
                Boolean login = (Boolean) rs.getOrDefault("rolcanlogin", false);
                Boolean replication = (Boolean) rs.getOrDefault("rolreplication", false);
                Boolean superuser = (Boolean) rs.getOrDefault("aws_superuser", false);
                Boolean admin = (Boolean) rs.getOrDefault("aws_admin", false);
                Boolean replicationAdmin = (Boolean) rs.getOrDefault("aws_repladmin", false);
                return login && (replication || superuser || admin || replicationAdmin);
            });
            if (!hasAuth) {
                throw new ListenerException(String.format("Postgres roles LOGIN and REPLICATION are not assigned to user: %s", config.getUsername()));
            }
            connected = true;
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        } finally {
            connectLock.unlock();
            close();
        }
    }

    @Override
    public void close() {
        try {
            connectLock.lock();
            connected = false;
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        } finally {
            connectLock.unlock();
        }
    }

    private void connect() throws SQLException {
        if (connectorFactory.isAlive(connectorConfig)) {
            config = (DatabaseConfig) connectorConfig;
            connectorMapper = (DatabaseConnectorMapper) connectorFactory.connect(config);

            connection = DatabaseUtil.getConnection(config.getDriverClassName(), config.getUrl(), config.getUsername(), config.getPassword());
            PGConnection replConnection = connection.unwrap(PGConnection.class);
            replConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(DEFAULT_SLOT_NAME)
                    .withOutputPlugin(DEFAULT_PLUGIN_NAME)
                    .make();
            stream = replConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(DEFAULT_SLOT_NAME)
                    .start();
        }
    }
}
