package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.listener.postgresql.enums.MessageTypeEnum;
import org.postgresql.jdbc.PgResultSet;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:00
 */
public class PgOutputMessageDecoder extends AbstractMessageDecoder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final LocalDateTime PG_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);

    private static final String GET_TABLE_SCHEMA = "select oid,relname as tableName from pg_class t inner join (select ns.oid as nspoid, ns.nspname from pg_namespace ns where ns.nspname = (select (current_schemas(false))[s.r] from generate_series(1, array_upper(current_schemas(false), 1)) as s(r))) as n on n.nspoid = t.relnamespace";

    private static final Map<Integer, TableId> tables = new LinkedHashMap<>();

    @Override
    public void postProcessBeforeInitialization(DatabaseConnectorMapper connectorMapper) {
        initPublication(connectorMapper);
        readSchema(connectorMapper);
    }

    @Override
    public RowChangedEvent processMessage(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
        }

        RowChangedEvent event = null;
        MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
        switch (type) {
            case UPDATE:
                event = parseUpdate(buffer);
                break;

            case INSERT:
                event = parseInsert(buffer);
                break;

            case DELETE:
                event = parseDelete(buffer);
                break;

            case BEGIN:
                long beginLsn = buffer.getLong();
                long beginTs = buffer.getLong();
                long xid = buffer.getInt();
                logger.info("Begin LSN {}, timestamp {}, xid {} - {}", beginLsn, PG_EPOCH.plusNanos(beginTs * 1000L), xid, beginTs);
                break;

            case COMMIT:
                buffer.get();
                long commitLsn = buffer.getLong();
                long commitEndLsn = buffer.getLong();
                long commitTs = buffer.getLong();
                logger.info("Commit: LSN {}, end LSN {}, ts {}", commitLsn, commitEndLsn, PG_EPOCH.plusNanos(commitTs * 1000L));
                break;

            default:
                logger.info("Type {} not implemented", type.name());
        }

        if (null != event) {
            logger.info(event.toString());
        }

        return null;
    }

    @Override
    public String getOutputPlugin() {
        return MessageDecoderEnum.PG_OUTPUT.getType();
    }

    @Override
    public void withSlotOption(ChainedLogicalStreamBuilder builder) {
        builder.withSlotOption("proto_version", 1);
        builder.withSlotOption("publication_names", getPubName());
    }

    private String getPubName() {
        return String.format("dbs_pub_%s_%s", config.getSchema(), config.getUsername());
    }

    private void initPublication(DatabaseConnectorMapper connectorMapper) {
        String pubName = getPubName();
        String selectPublication = String.format("SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s'", pubName);
        Integer count = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(selectPublication, Integer.class));
        if (0 < count) {
            return;
        }

        logger.info("Creating new publication '{}' for plugin '{}'", pubName, getOutputPlugin());
        try {
            String createPublication = String.format("CREATE PUBLICATION %s FOR ALL TABLES", pubName);
            logger.info("Creating Publication with statement '{}'", createPublication);
            connectorMapper.execute(databaseTemplate -> {
                databaseTemplate.execute(createPublication);
                return true;
            });
        } catch (Exception e) {
            throw new ListenerException(e.getCause());
        }
    }

    private void readSchema(DatabaseConnectorMapper connectorMapper) {
        List<Map> schemas = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(GET_TABLE_SCHEMA));
        if (!CollectionUtils.isEmpty(schemas)) {
            schemas.forEach(map -> {
                Long oid = (Long) map.get("oid");
                String tableName = (String) map.get("tableName");
                tables.put(oid.intValue(), new TableId(oid.intValue(), tableName));
            });
        }

        try {
            Connection connection = connectorMapper.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();

            // read column
            for (TableId tableId : tables.values()) {
                ResultSet rs = metaData.getColumns("", config.getSchema(), tableId.tableName, null);
                PgResultSet pgResultSet = (PgResultSet) rs;
                while (pgResultSet.next()) {
                    pgResultSet.getRow();
                }
            }
        } catch (Exception e) {
            throw new ListenerException(e.getCause());
        }
    }

    private RowChangedEvent parseDelete(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        logger.info("Delete table {}", tables.get(relationId).tableName);

        List<Object> data = new ArrayList<>();
        String newTuple = new String(new byte[]{buffer.get()}, 0, 1);

        switch (newTuple) {
            case "K":
                readTupleData(buffer, data);
                break;
            default:
                logger.info("K not set, got instead {}", newTuple);
        }
        return new RowChangedEvent(tables.get(relationId).tableName, ConnectorConstant.OPERTION_INSERT, data, Collections.EMPTY_LIST);
    }

    private RowChangedEvent parseInsert(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        logger.info("Insert table {}", tables.get(relationId).tableName);

        List<Object> data = new ArrayList<>();
        String newTuple = new String(new byte[]{buffer.get()}, 0, 1);
        switch (newTuple) {
            case "N":
                readTupleData(buffer, data);
                break;
            default:
                logger.info("N not set, got instead {}", newTuple);
        }
        return new RowChangedEvent(tables.get(relationId).tableName, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, data);
    }

    private RowChangedEvent parseUpdate(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        logger.info("Update table {}", tables.get(relationId).tableName);

        List<Object> data = new ArrayList<>();
        String newTuple = new String(new byte[]{buffer.get()}, 0, 1);
        switch (newTuple) {
            case "K":
                logger.info("Key update");
                logger.info("Old Key");
                readTupleData(buffer, data);
                break;
            case "O":
                logger.info("Value update");
                logger.info("Old Value");
                readTupleData(buffer, data);
                break;
            case "N":
                readTupleData(buffer, data);
                break;
            default:
                logger.info("K or O Byte1 not set, got instead {}", newTuple);
        }

        return new RowChangedEvent(tables.get(relationId).tableName, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, data);
    }

    private void readTupleData(ByteBuffer msg, List<Object> data) {
        short nColumn = msg.getShort();
        for (int n = 0; n < nColumn; n++) {
            String tupleContentType = new String(new byte[]{msg.get()}, 0, 1);
            if (tupleContentType.equals("t")) {
                int size = msg.getInt();
                byte[] text = new byte[size];

                for (int z = 0; z < size; z++) {
                    text[z] = msg.get();
                }
                String content = new String(text, 0, size);
                data.add(content);
                continue;
            }

            if (tupleContentType.equals("n")) {
                data.add(null);
                continue;
            }

            if (tupleContentType.equals("u")) {
                data.add("TOASTED");
            }
        }
    }

    final class TableId {
        Integer oid;
        String tableName;

        public TableId(Integer oid, String tableName) {
            this.oid = oid;
            this.tableName = tableName;
        }
    }

}