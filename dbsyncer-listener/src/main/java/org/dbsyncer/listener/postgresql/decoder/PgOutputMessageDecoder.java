package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.listener.postgresql.enums.MessageTypeEnum;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.nio.ByteBuffer;
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
    private static final String GET_TABLE_SCHEMA = "select oid,relname as tableName from pg_class t inner join (select ns.oid as nspoid, ns.nspname from pg_namespace ns where ns.nspname = (select (current_schemas(false))[s.r] from generate_series(1, array_upper(current_schemas(false), 1)) as s(r))) as n on n.nspoid = t.relnamespace where relkind = 'r'";
    private static final Map<Integer, TableId> tables = new LinkedHashMap<>();
    private ConnectorFactory connectorFactory;
    private DatabaseConnectorMapper connectorMapper;

    @Override
    public void postProcessBeforeInitialization(ConnectorFactory connectorFactory, DatabaseConnectorMapper connectorMapper) {
        this.connectorFactory = connectorFactory;
        this.connectorMapper = connectorMapper;
        initPublication();
        readSchema();
    }

    @Override
    public RowChangedEvent processMessage(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
        }

        MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
        switch (type) {
            case UPDATE:
            case INSERT:
            case DELETE:
                return parseData(type, buffer);

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

    private void initPublication() {
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

    private void readSchema() {
        List<Map> schemas = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(GET_TABLE_SCHEMA));
        if (!CollectionUtils.isEmpty(schemas)) {
            schemas.forEach(map -> {
                Long oid = (Long) map.get("oid");
                String tableName = (String) map.get("tableName");
                MetaInfo metaInfo = connectorFactory.getMetaInfo(connectorMapper, tableName);
                Assert.notEmpty(metaInfo.getColumn(), String.format("The table column for '%s' must not be empty.", tableName));
                tables.put(oid.intValue(), new TableId(oid.intValue(), tableName, metaInfo.getColumn()));
            });
        }
    }

    private RowChangedEvent parseData(MessageTypeEnum type, ByteBuffer buffer) {
        final int relationId = buffer.getInt();
        final TableId tableId = tables.get(relationId);
        if (null != tableId) {
            String newTuple = new String(new byte[]{buffer.get()}, 0, 1);
            switch (newTuple) {
                case "N":
                case "K":
                case "O":
                    List<Object> data = new ArrayList<>();
                    readTupleData(tableId, buffer, data);
                    if (MessageTypeEnum.DELETE == type) {
                        return new RowChangedEvent(tableId.tableName, type.name(), data, Collections.EMPTY_LIST);
                    }
                    return new RowChangedEvent(tableId.tableName, type.name(), Collections.EMPTY_LIST, data);

                default:
                    logger.info("N, K, O not set, got instead {}", newTuple);
            }
        }
        return null;
    }

    private void readTupleData(TableId tableId, ByteBuffer msg, List<Object> data) {
        short nColumn = msg.getShort();
        if (nColumn != tableId.fields.size()) {
            logger.warn("The column size of table '{}' is {}, but we has been received column size is {}.", tableId.tableName, tableId.fields.size(), nColumn);

            // The table schema has been changed, we should be get a new table schema from db.
            MetaInfo metaInfo = connectorFactory.getMetaInfo(connectorMapper, tableId.tableName);
            if (CollectionUtils.isEmpty(metaInfo.getColumn())) {
                throw new ListenerException(String.format("The table column for '%s' is empty.", tableId.tableName));
            }
            tableId.fields = metaInfo.getColumn();
            return;
        }

        for (int n = 0; n < nColumn; n++) {
            String type = new String(new byte[]{msg.get()}, 0, 1);
            switch (type) {
                case "t":
                    int size = msg.getInt();
                    byte[] text = new byte[size];
                    for (int z = 0; z < size; z++) {
                        text[z] = msg.get();
                    }
                    data.add(resolveValue(tableId.fields.get(n).getTypeName(), new String(text, 0, size)));
                    break;

                case "n":
                    data.add(null);
                    break;

                case "u":
                    data.add("TOASTED");
                    break;
                default:
                    logger.info("t, n, u not set, got instead {}", type);
            }
        }
    }

    final class TableId {
        Integer oid;
        String tableName;
        List<Field> fields;

        public TableId(Integer oid, String tableName, List<Field> fields) {
            this.oid = oid;
            this.tableName = tableName;
            this.fields = fields;
        }
    }

}