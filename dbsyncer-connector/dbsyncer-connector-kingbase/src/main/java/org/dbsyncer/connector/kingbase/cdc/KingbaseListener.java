/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kingbase.cdc;

import com.kingbase8.KBConnection;
import com.kingbase8.KBProperty;
import com.kingbase8.replication.KBReplicationStream;
import com.kingbase8.replication.LogSequenceNumber;
import com.kingbase8.replication.fluent.logical.ChainedLogicalStreamBuilder;
import com.kingbase8.util.KSQLException;
import com.kingbase8.util.KSQLState;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.connector.kingbase.KingbaseException;
import org.dbsyncer.connector.kingbase.constant.KingbaseConfigConstant;
import org.dbsyncer.connector.postgresql.decoder.MessageDecoder;
import org.dbsyncer.connector.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:30
 */
public class KingbaseListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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

    private static final String GET_WAL_LEVEL = "SHOW WAL_LEVEL";
    private static final String DEFAULT_WAL_LEVEL = "logical";
    private static final String LSN_POSITION = "position";
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private DatabaseConfig config;
    private DatabaseConnectorInstance instance;
    private Connection connection;
    private KBReplicationStream stream;
    private boolean dropSlotOnClose;
    private MessageDecoder messageDecoder;
    private Worker worker;
    private LogSequenceNumber startLsn;
    private org.postgresql.replication.LogSequenceNumber decoderStartLsn;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("KingbaseExtractor is already started");
                return;
            }

            instance = getConnectorInstance();
            config = instance.getConfig();

            final String walLevel = instance.execute(databaseTemplate -> databaseTemplate.queryForObject(GET_WAL_LEVEL, String.class));
            if (!DEFAULT_WAL_LEVEL.equalsIgnoreCase(walLevel)) {
                throw new KingbaseException(String.format("Kingbase server wal_level must be \"%s\" but is: %s", DEFAULT_WAL_LEVEL, walLevel));
            }

            final boolean hasAuth = instance.execute(databaseTemplate -> {
                Map rs = databaseTemplate.queryForMap(GET_ROLE);
                Boolean login = (Boolean) rs.getOrDefault("login", false);
                Boolean replication = (Boolean) rs.getOrDefault("replication", false);
                Boolean superuser = (Boolean) rs.getOrDefault("superuser", false);
                Boolean rdsSuperuser = (Boolean) rs.getOrDefault("rds_superuser", false);
                Boolean admin = (Boolean) rs.getOrDefault("admin", false);
                Boolean repAdmin = (Boolean) rs.getOrDefault("rep_admin", false);
                return login && (replication || superuser || rdsSuperuser || admin || repAdmin);
            });
            if (!hasAuth) {
                throw new KingbaseException(String.format("Kingbase roles LOGIN and REPLICATION are not assigned to user: %s", config.getUsername()));
            }

            Properties extInfo = config.getExtInfo();
            String pluginName = extInfo.getProperty(KingbaseConfigConstant.PLUGIN_NAME, KingbaseConfigConstant.DEFAULT_PLUGIN);
            if ("pgoutput".equalsIgnoreCase(pluginName)) {
                pluginName = KingbaseConfigConstant.DEFAULT_PLUGIN;
            }
            messageDecoder = MessageDecoderEnum.getMessageDecoder(pluginName);
            messageDecoder.setMetaId(metaId);
            messageDecoder.setConfig(config);
            messageDecoder.setDatabase(database);
            messageDecoder.setSchema(schema);
            messageDecoder.postProcessBeforeInitialization(connectorService, instance, database);
            dropSlotOnClose = BooleanUtil.toBoolean(extInfo.getProperty(KingbaseConfigConstant.DROP_SLOT_ON_CLOSE, "true"));

            connect();
            connected = true;

            worker = new Worker();
            worker.setName("kingbase-wal-parser-" + config.getUrl() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            DatabaseUtil.close(stream);
            DatabaseUtil.close(connection);
            throw new KingbaseException(e);
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
    public Map<String, String> captureSnapshot() {
        try {
            DatabaseConnectorInstance captureInstance = getConnectorInstance();
            String lsn = captureInstance.execute(databaseTemplate -> {
                try {
                    return databaseTemplate.queryForObject("select pg_current_wal_lsn()::text", String.class);
                } catch (Exception e) {
                    return databaseTemplate.queryForObject("select sys_current_wal_lsn()::text", String.class);
                }
            });
            if (lsn == null) {
                return Collections.emptyMap();
            }
            snapshot.put(LSN_POSITION, lsn);
            Map<String, String> captured = new HashMap<>(1);
            captured.put(LSN_POSITION, lsn);
            return captured;
        } catch (Exception e) {
            logger.error("捕获Kingbase LSN位点失败:{}", e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        snapshot.put(LSN_POSITION, String.valueOf(offset.getPosition()));
    }

    private void connect() throws SQLException {
        Properties props = new Properties();
        KBProperty.USER.set(props, config.getUsername());
        KBProperty.PASSWORD.set(props, config.getPassword());
        KBProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        KBProperty.REPLICATION.set(props, "database");
        KBProperty.PREFER_QUERY_MODE.set(props, "simple");
        connection = DriverManager.getConnection(config.getUrl(), props);
        Assert.notNull(connection, "Unable to get connection.");

        KBConnection kbConnection = connection.unwrap(KBConnection.class);
        createReplicationSlot(kbConnection);
        createReplicationStream(kbConnection);

        sleepInMills(10L);
    }

    private void createReplicationStream(KBConnection kbConnection) throws SQLException {
        ChainedLogicalStreamBuilder streamBuilder = kbConnection.getReplicationAPI().replicationStream().logical()
                .withSlotName(messageDecoder.getSlotName()).withStartPosition(startLsn)
                .withStatusInterval(10, TimeUnit.SECONDS)
                .withSlotOption("include-xids", true)
                .withSlotOption("skip-empty-xacts", true);
        this.stream = streamBuilder.start();
    }

    private void createReplicationSlot(KBConnection kbConnection) throws SQLException {
        String slotName = messageDecoder.getSlotName();
        String plugin = messageDecoder.getOutputPlugin();
        boolean existSlot = instance.execute(databaseTemplate ->
                databaseTemplate.queryForObject(GET_SLOT, new Object[]{database, slotName, plugin}, Integer.class) > 0);
        if (!existSlot) {
            kbConnection.getReplicationAPI().createReplicationSlot().logical()
                    .withSlotName(slotName).withOutputPlugin(plugin).make();
            sleepInMills(300);
        }

        if (!snapshot.containsKey(LSN_POSITION)) {
            LogSequenceNumber lsn = instance.execute(databaseTemplate ->
                    LogSequenceNumber.valueOf(databaseTemplate.queryForObject(GET_RESTART_LSN,
                            new Object[]{database, slotName, plugin}, String.class)));
            if (null == lsn || lsn.asLong() == 0) {
                throw new KingbaseException("No maximum LSN recorded in the database");
            }
            snapshot.put(LSN_POSITION, lsn.asString());
            super.forceFlushEvent();
        }

        this.startLsn = LogSequenceNumber.valueOf(snapshot.get(LSN_POSITION));
        this.decoderStartLsn = org.postgresql.replication.LogSequenceNumber.valueOf(this.startLsn.asString());
    }

    private void dropReplicationSlot() {
        if (!dropSlotOnClose) {
            return;
        }

        final String slotName = messageDecoder.getSlotName();
        final int attempts = 3;
        for (int i = 0; i < attempts; i++) {
            try {
                instance.execute(databaseTemplate -> {
                    try {
                        databaseTemplate.execute(String.format("select sys_drop_replication_slot('%s')", slotName));
                    } catch (Exception e) {
                        databaseTemplate.execute(String.format("select pg_drop_replication_slot('%s')", slotName));
                    }
                    return true;
                });
                break;
            } catch (Exception e) {
                if (e.getCause() instanceof KSQLException) {
                    KSQLException ex = (KSQLException) e.getCause();
                    if (KSQLState.OBJECT_IN_USE.getState().equals(ex.getSQLState())) {
                        if (i < attempts - 1) {
                            logger.debug("Cannot drop replication slot '{}' because it's still in use", slotName);
                            continue;
                        }
                        logger.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                        break;
                    }
                    if (KSQLState.UNDEFINED_OBJECT.getState().equals(ex.getSQLState())) {
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
            logger.info("Recover logical replication success, slot:{}, plugin:{}, cost:{}seconds",
                    messageDecoder.getSlotName(), messageDecoder.getOutputPlugin(), (e - s) / 1000);
        } finally {
            connectLock.unlock();
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    ByteBuffer msg = stream.readPending();
                    if (msg == null) {
                        sleepInMills(10L);
                        continue;
                    }

                    LogSequenceNumber lsn = stream.getLastReceiveLSN();
                    if (messageDecoder.skipMessage(msg, decoderStartLsn,
                            org.postgresql.replication.LogSequenceNumber.valueOf(lsn.asString()))) {
                        continue;
                    }

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

                    stream.setAppliedLSN(lsn);
                    stream.setFlushedLSN(lsn);
                    stream.forceUpdateStatus();
                } catch (IllegalStateException | KingbaseException e) {
                    logger.error(e.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    recover();
                }
            }
        }
    }
}
