package org.dbsyncer.listener.postgresql;

import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.common.util.RandomUtil;
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

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String GET_SLOT = "select count(1) from pg_replication_slots where database = ? and slot_name = ? and plugin = ?";
    private static final String GET_ROLE = "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin, CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin FROM pg_roles r WHERE r.rolname = current_user";
    private static final String GET_DATABASE = "SELECT current_database()";
    private static final String GET_WAL_LEVEL = "SHOW WAL_LEVEL";
    private static final String DEFAULT_WAL_LEVEL = "logical";
    private static final String PLUGIN_NAME = "pluginName";
    private static final String LSN_POSITION = "position";
    private static final String DROP_SLOT_ON_CLOSE = "dropSlotOnClose";
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private DatabaseConfig config;
    private DatabaseConnectorMapper connectorMapper;
    private Connection connection;
    private PGReplicationStream stream;
    private boolean dropSlotOnClose;
    private MessageDecoder messageDecoder;
    private Worker worker;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("PostgreSQLExtractor is already started");
                return;
            }

            connectorMapper = (DatabaseConnectorMapper) connectorFactory.connect(connectorConfig);
            config = connectorMapper.getConfig();

            final String walLevel = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_WAL_LEVEL, String.class));
            if (!DEFAULT_WAL_LEVEL.equals(walLevel)) {
                throw new ListenerException(String.format("Postgres server wal_level property must be \"%s\" but is: %s", DEFAULT_WAL_LEVEL, walLevel));
            }

            final boolean hasAuth = connectorMapper.execute(databaseTemplate -> {
                Map rs = databaseTemplate.queryForMap(GET_ROLE);
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

            messageDecoder = MessageDecoderEnum.getMessageDecoder(config.getProperties().get(PLUGIN_NAME));
            messageDecoder.setConfig(config);
            String dropSlot = config.getProperties().get(DROP_SLOT_ON_CLOSE);
            dropSlotOnClose = null != dropSlot ? BooleanUtil.toBoolean(dropSlot) : true;

            connect();
            connected = true;

            worker = new Worker();
            worker.setName(new StringBuilder("wal-parser-").append(config.getUrl()).append("_").append(RandomUtil.nextInt(1, 100)).toString());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
            throw new ListenerException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            connected = false;
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
                worker = null;
            }
            dropReplicationSlot();
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        }
    }

    private void connect() throws SQLException {
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

        createReplicationSlot(pgConnection);
        createReplicationStream(pgConnection);

        sleepInMills(10L);
    }

    private LogSequenceNumber readLastLsn() throws SQLException {
        if (!snapshot.containsKey(LSN_POSITION)) {
            LogSequenceNumber lsn = currentXLogLocation();
            if (null == lsn || lsn.asLong() == 0) {
                throw new ListenerException("No maximum LSN recorded in the database");
            }
            snapshot.put(LSN_POSITION, lsn.asString());
        }

        return LogSequenceNumber.valueOf(snapshot.get(LSN_POSITION));
    }

    private void createReplicationStream(PGConnection pgConnection) throws SQLException {
        LogSequenceNumber lsn = readLastLsn();
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
        String database = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_DATABASE, String.class));
        String slotName = messageDecoder.getSlotName();
        String plugin = messageDecoder.getOutputPlugin();
        boolean existSlot = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_SLOT, new Object[]{database, slotName, plugin}, Integer.class) > 0);
        if (!existSlot) {
            pgConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(slotName)
                    .withOutputPlugin(plugin)
                    .make();
        }
    }

    private void dropReplicationSlot() {
        if (dropSlotOnClose) {
            connectorMapper.execute(databaseTemplate -> {
                databaseTemplate.execute(String.format("select pg_drop_replication_slot('%s')", messageDecoder.getSlotName()));
                return true;
            });
        }
    }

    private LogSequenceNumber currentXLogLocation() throws SQLException {
        int majorVersion = connection.getMetaData().getDatabaseMajorVersion();
        String sql = majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()";
        return connectorMapper.execute(databaseTemplate -> LogSequenceNumber.valueOf(databaseTemplate.queryForObject(sql, String.class)));
    }

    private void recover() {
        connectLock.lock();
        try {
            long s = Instant.now().toEpochMilli();
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
            stream = null;
            connection = null;

            while (true && connected) {
                try {
                    connect();
                    break;
                } catch (Exception e) {
                    logger.error("Recover streaming occurred error");
                    DatabaseUtil.close(stream);
                    DatabaseUtil.close(connection);
                    sleepInMills(3000L);
                }
            }
            long e = Instant.now().toEpochMilli();
            logger.info("Recover logical replication success, slot:{}, plugin:{}, cost:{}seconds", messageDecoder.getSlotName(), messageDecoder.getOutputPlugin(), (e - s) / 1000);
        } finally {
            connectLock.unlock();
        }
    }

    private void sleepInMills(long timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    // non blocking receive message
                    ByteBuffer msg = stream.readPending();

                    if (msg == null) {
                        sleepInMills(10L);
                        continue;
                    }
                    int offset = msg.arrayOffset();
                    byte[] source = msg.array();
                    int length = source.length - offset;
                    logger.info(new String(source, offset, length));

                    LogSequenceNumber lsn = stream.getLastReceiveLSN();
                    if (lsn.asLong() > 0) {
                        snapshot.put(LSN_POSITION, lsn.asString());
                    }
                    // feedback
                    stream.setAppliedLSN(lsn);
                    stream.setFlushedLSN(lsn);
                    stream.forceUpdateStatus();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    recover();
                }
            }
        }

    }
}