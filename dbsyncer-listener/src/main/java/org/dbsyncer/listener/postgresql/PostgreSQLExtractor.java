package org.dbsyncer.listener.postgresql;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/10 22:36
 */
public class PostgreSQLExtractor extends AbstractExtractor {

    private static final String GET_SLOT = "select count(1) from pg_replication_slots where database = ? and slot_name = ? and plugin = ?";

    private static final String GET_VALIDATION = "SELECT 1";
    private static final String GET_ROLE = "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin FROM pg_roles r WHERE r.rolname = current_user";
    private static final String GET_WAL_LEVEL = "SHOW WAL_LEVEL";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String DEFAULT_WAL_LEVEL = "logical";
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private DatabaseConfig config;
    private DatabaseConnectorMapper connectorMapper;
    private Connection connection;
    private PGReplicationStream stream;
    private MessageDecoder messageDecoder;

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

    private void connect() throws SQLException, InstantiationException, IllegalAccessException {
        config = (DatabaseConfig) connectorConfig;
        connectorMapper = (DatabaseConnectorMapper) connectorFactory.connect(config);

        Properties props = new Properties();
        PGProperty.USER.set(props, config.getUsername());
        PGProperty.PASSWORD.set(props, config.getPassword());
        // Postgres 9.4发布逻辑复制功能
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        connection = DriverManager.getConnection(config.getUrl(), props);
        Assert.notNull(connection, "Unable to get connection.");

        PGConnection pgConnection = connection.unwrap(PGConnection.class);
        messageDecoder = MessageDecoderEnum.getMessageDecoder(MessageDecoderEnum.TEST_DECODING.getType());
        messageDecoder.setMessageDecoderContext(new MessageDecoderContext(config));

        createReplicationSlot(pgConnection);
        createReplicationStream(pgConnection);
    }

    private void createReplicationStream(PGConnection pgConnection) throws SQLException {
        LogSequenceNumber lsn = currentXLogLocation();
        ChainedLogicalStreamBuilder streamBuilder = pgConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(messageDecoder.getSlotName())
                .withStartPosition(lsn)
                .withStatusInterval(10, TimeUnit.SECONDS);

        messageDecoder.withSlotOption(streamBuilder);
        this.stream = streamBuilder.start();
    }

    private void createReplicationSlot(PGConnection pgConnection) throws SQLException {
        String slotName = messageDecoder.getSlotName();
        boolean existSlot = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_SLOT, new Object[]{config.getSchema(), slotName}, Integer.class) > 0);
        if (!existSlot) {
            pgConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(slotName)
                    .withOutputPlugin(messageDecoder.getOutputPlugin())
                    .make();
        }
    }

    private LogSequenceNumber currentXLogLocation() throws SQLException {
        int majorVersion = connection.getMetaData().getDatabaseMajorVersion();
        String sql = majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()";
        return connectorMapper.execute(databaseTemplate -> LogSequenceNumber.valueOf(databaseTemplate.queryForObject(sql, String.class)));
    }

}
