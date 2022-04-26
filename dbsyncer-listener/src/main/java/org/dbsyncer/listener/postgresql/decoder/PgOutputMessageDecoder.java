package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.listener.postgresql.enums.MessageTypeEnum;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:00
 */
public class PgOutputMessageDecoder extends AbstractMessageDecoder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final LocalDateTime PG_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);

    @Override
    public void postProcessBeforeInitialization(DatabaseConnectorMapper connectorMapper) {
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

        // TODO read table schema
    }

    @Override
    public boolean skipMessage(ByteBuffer buffer, LogSequenceNumber startLsn, LogSequenceNumber lastReceiveLsn) {
        if (super.skipMessage(buffer, startLsn, lastReceiveLsn)) {
            return true;
        }
        int position = buffer.position();
        try {
            MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
            switch (type) {
                case BEGIN:
                case COMMIT:
                case RELATION:
                case TRUNCATE:
                case TYPE:
                case ORIGIN:
                case NONE:
                    return true;
                default:
                    // TABLE|INSERT|UPDATE|DELETE
                    return false;
            }
        } finally {
            buffer.position(position);
        }
    }

    @Override
    public RowChangedEvent processMessage(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
        }

        RowChangedEvent event = null;
        MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
        switch (type) {
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

            case UPDATE:
                event = parseUpdate(buffer);
                break;

            case INSERT:
                event = parseInsert(buffer);
                break;

            case DELETE:
                event = parseDelete(buffer);
                break;

            default:
                logger.info("Type {} not implemented", type.name());
        }

        if (null != event) {
            logger.info(event.toString());
        }

        return null;
    }

    private RowChangedEvent parseDelete(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        logger.info("Delete table {}", relationId);

        List<Object> data = new ArrayList<>();
        String newTuple = new String(new byte[]{buffer.get()}, 0, 1);

        switch (newTuple) {
            case "K":
                readTupleData(buffer, data);
                break;
            default:
                logger.info("K not set, got instead {}", newTuple);
        }
        return new RowChangedEvent(String.valueOf(relationId), ConnectorConstant.OPERTION_INSERT, data, Collections.EMPTY_LIST);
    }

    private RowChangedEvent parseInsert(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        logger.info("Insert table {}", relationId);

        List<Object> data = new ArrayList<>();
        String newTuple = new String(new byte[]{buffer.get()}, 0, 1);
        switch (newTuple) {
            case "N":
                readTupleData(buffer, data);
                break;
            default:
                logger.info("N not set, got instead {}", newTuple);
        }
        return new RowChangedEvent(String.valueOf(relationId), ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, data);
    }

    private RowChangedEvent parseUpdate(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        logger.info("Update table {}", relationId);

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

        return new RowChangedEvent(String.valueOf(relationId), ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, data);
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

}