/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.cdc;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.connector.postgresql.constant.PostgreSQLConfigConstant;
import org.dbsyncer.connector.postgresql.decoder.MessageDecoder;
import org.dbsyncer.connector.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.util.DatabaseUtil;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
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
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-10 22:36
 */
public class PostgreSQLListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // spotless:off
    private static final String GET_SLOT =
            "select count(1) from pg_replication_slots where database = ? and slot_name = ? and plugin = ?";
    private static final String GET_RESTART_LSN =
            "select restart_lsn from pg_replication_slots where database = ? and slot_name = ? and plugin = ?";
    private static final String GET_ROLE =
            "SELECT r.rolcanlogin AS login,r.rolreplication AS replication,u.usesuper as superuser,"
                + "        CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)"
                + "            WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS rds_superuser,"
                + "        CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)"
                + "            WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS admin,"
                + "        CAST(array_position(ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)"
                + "            WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS rep_admin"
                + "    FROM pg_user u left join pg_roles r on u.usename=r.rolname"
                + "    WHERE u.usename = current_user";
    // spotless:on

    private static final String GET_WAL_LEVEL = "SHOW WAL_LEVEL";
    private static final String DEFAULT_WAL_LEVEL = "logical";
    private static final String LSN_POSITION = "position";
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private DatabaseConfig config;
    private DatabaseConnectorInstance instance;
    private Connection connection;
    private PGReplicationStream stream;
    private boolean dropSlotOnClose;
    private MessageDecoder messageDecoder;
    private Worker worker;
    private LogSequenceNumber startLsn;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("PostgreSQLExtractor is already started");
                return;
            }

            instance = getConnectorInstance();
            config = instance.getConfig();

            final String walLevel = instance.execute(databaseTemplate->databaseTemplate.queryForObject(GET_WAL_LEVEL, String.class));
            if (!DEFAULT_WAL_LEVEL.equals(walLevel)) {
                throw new PostgreSQLException(String.format("Postgres server wal_level property must be \"%s\" but is: %s", DEFAULT_WAL_LEVEL, walLevel));
            }

            final boolean hasAuth = instance.execute(databaseTemplate-> {
                Map rs = databaseTemplate.queryForMap(GET_ROLE);
                Boolean login = (Boolean) rs.getOrDefault("login", false);
                Boolean replication = (Boolean) rs.getOrDefault("replication", false);
                Boolean superuser = (Boolean) rs.getOrDefault("superuser", false);
                // 阿里云 RDS from PostgreSQL
                Boolean rds_superuser = (Boolean) rs.getOrDefault("rds_superuser", false);
                Boolean admin = (Boolean) rs.getOrDefault("admin", false);
                Boolean repAdmin = (Boolean) rs.getOrDefault("rep_admin", false);
                return login && (replication || superuser || rds_superuser || admin || repAdmin);
            });
            if (!hasAuth) {
                throw new PostgreSQLException(String.format("Postgres roles LOGIN and REPLICATION are not assigned to user: %s", config.getUsername()));
            }

            Properties extInfo = config.getExtInfo();
            messageDecoder = MessageDecoderEnum.getMessageDecoder(extInfo.getProperty(PostgreSQLConfigConstant.PLUGIN_NAME));
            messageDecoder.setMetaId(metaId);
            messageDecoder.setConfig(config);
            messageDecoder.setDatabase(database);
            messageDecoder.setSchema(schema);
            messageDecoder.postProcessBeforeInitialization(connectorService, instance, database);
            dropSlotOnClose = BooleanUtil.toBoolean(extInfo.getProperty(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE, "true"));

            connect();
            connected = true;

            worker = new Worker();
            worker.setName("wal-parser-" + config.getUrl() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
            throw new PostgreSQLException(e);
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
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
            dropReplicationSlot();
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        snapshot.put(LSN_POSITION, String.valueOf(offset.getPosition()));
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

    private void createReplicationStream(PGConnection pgConnection) throws SQLException {
        ChainedLogicalStreamBuilder streamBuilder = pgConnection.getReplicationAPI().replicationStream().logical().withSlotName(messageDecoder.getSlotName()).withStartPosition(startLsn)
                .withStatusInterval(10, TimeUnit.SECONDS);

        messageDecoder.withSlotOption(streamBuilder);
        this.stream = streamBuilder.start();
    }

    private void createReplicationSlot(PGConnection pgConnection) throws SQLException {
        String slotName = messageDecoder.getSlotName();
        String plugin = messageDecoder.getOutputPlugin();
        boolean existSlot = instance.execute(databaseTemplate->databaseTemplate.queryForObject(GET_SLOT, new Object[]{database, slotName, plugin}, Integer.class) > 0);
        if (!existSlot) {
            pgConnection.getReplicationAPI().createReplicationSlot().logical().withSlotName(slotName).withOutputPlugin(plugin).make();

            // wait for create replication slot to have finished
            sleepInMills(300);
        }

        if (!snapshot.containsKey(LSN_POSITION)) {
            LogSequenceNumber lsn = instance
                    .execute(databaseTemplate->LogSequenceNumber.valueOf(databaseTemplate.queryForObject(GET_RESTART_LSN, new Object[]{database, slotName, plugin}, String.class)));
            if (null == lsn || lsn.asLong() == 0) {
                throw new PostgreSQLException("No maximum LSN recorded in the database");
            }
            snapshot.put(LSN_POSITION, lsn.asString());
            super.forceFlushEvent();
        }

        this.startLsn = LogSequenceNumber.valueOf(snapshot.get(LSN_POSITION));
    }

    private void dropReplicationSlot() {
        if (!dropSlotOnClose) {
            return;
        }

        final String slotName = messageDecoder.getSlotName();
        final int ATTEMPTS = 3;
        for (int i = 0; i < ATTEMPTS; i++) {
            try {
                instance.execute(databaseTemplate-> {
                    databaseTemplate.execute(String.format("select pg_drop_replication_slot('%s')", slotName));
                    return true;
                });
                break;
            } catch (Exception e) {
                if (e.getCause() instanceof PSQLException) {
                    PSQLException ex = (PSQLException) e.getCause();
                    if (PSQLState.OBJECT_IN_USE.getState().equals(ex.getSQLState())) {
                        if (i < ATTEMPTS - 1) {
                            logger.debug("Cannot drop replication slot '{}' because it's still in use", slotName);
                            continue;
                        }
                        logger.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                        break;
                    }

                    if (PSQLState.UNDEFINED_OBJECT.getState().equals(ex.getSQLState())) {
                        logger.debug("Replication slot {} has already been dropped", slotName);
                        break;
                    }

                    logger.error("Unexpected error while attempting to drop replication slot", ex);
                }
            }
        }
    }

    private void recover() {
        connectLock.lock();
        try {
            long s = Instant.now().toEpochMilli();
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
            stream = null;
            connection = null;

            while (connected) {
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

                    LogSequenceNumber lsn = stream.getLastReceiveLSN();
                    if (messageDecoder.skipMessage(msg, startLsn, lsn)) {
                        continue;
                    }

                    // process decoder
                    RowChangedEvent event = messageDecoder.processMessage(msg);
                    if (event != null && filterTable.contains(event.getSourceTableName())) {
                        event.setPosition(lsn.asString());
                        while (connected) {
                            try {
                                sendChangedEvent(event);
                                break;
                            } catch (QueueOverflowException ex) {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(1);
                                } catch (InterruptedException exe) {
                                    logger.error(exe.getMessage(), exe);
                                }
                            }
                        }
                    }

                    // feedback
                    stream.setAppliedLSN(lsn);
                    stream.setFlushedLSN(lsn);
                    stream.forceUpdateStatus();
                } catch (IllegalStateException | PostgreSQLException e) {
                    logger.error(e.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    recover();
                }
            }
        }

    }

}