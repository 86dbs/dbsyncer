/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.decoder.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.connector.postgresql.decoder.AbstractMessageDecoder;
import org.dbsyncer.connector.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.connector.postgresql.enums.MessageTypeEnum;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.spi.ConnectorService;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-10 22:36
 */
public class PgOutputMessageDecoder extends AbstractMessageDecoder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final LocalDateTime PG_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    private static final String GET_TABLE_SCHEMA = "select t.oid,t.relname as tableName from pg_class t inner join (select ns.oid as nspoid, ns.nspname from pg_namespace ns where ns.nspname = '%s') as n on n.nspoid = t.relnamespace where relkind = 'r'";
    private static final Map<Integer, TableId> tables = new ConcurrentHashMap<>();
    private ConnectorService connectorService;
    private DatabaseConnectorInstance connectorInstance;

    @Override
    public void postProcessBeforeInitialization(ConnectorService connectorService, DatabaseConnectorInstance connectorInstance, String database) {
        this.connectorService = connectorService;
        this.connectorInstance = connectorInstance;
        this.database = database;
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
        return String.format("dbs_pub_%s_%s", schema, config.getUsername()).toLowerCase();
    }

    private void initPublication() {
        String pubName = getPubName();
        String selectPublication = String.format("SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s'", pubName);
        Integer count = connectorInstance.execute(databaseTemplate->databaseTemplate.queryForObject(selectPublication, Integer.class));
        if (0 < count) {
            return;
        }

        logger.info("Creating new publication '{}' for plugin '{}'", pubName, getOutputPlugin());
        try {
            String createPublication = String.format("CREATE PUBLICATION %s FOR ALL TABLES", pubName);
            logger.info("Creating Publication with statement '{}'", createPublication);
            connectorInstance.execute(databaseTemplate-> {
                databaseTemplate.execute(createPublication);
                return true;
            });
        } catch (Exception e) {
            throw new PostgreSQLException(e.getCause());
        }
    }

    private void readSchema() {
        final String querySchema = String.format(GET_TABLE_SCHEMA, schema);
        List<Map> schemas = connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(querySchema));
        if (!CollectionUtils.isEmpty(schemas)) {
            schemas.forEach(map-> {
                Long oid = (Long) map.get("oid");
                String tableName = (String) map.get("tableName");
                MetaInfo metaInfo = getMetaInfo(tableName);
                Assert.notEmpty(metaInfo.getColumn(), String.format("The table column for '%s' must not be empty.", tableName));
                tables.put(oid.intValue(), new TableId(oid.intValue(), tableName, metaInfo.getColumn()));
            });
        }
    }

    private RowChangedEvent parseData(MessageTypeEnum type, ByteBuffer buffer) {
        final int relationId = buffer.getInt();
        final TableId tableId = tables.get(relationId);
        if (null != tableId) {
            // UPDATE 场景下可能包含旧值(O)和新值(N)，需要合并处理
            if (type == MessageTypeEnum.UPDATE) {
                return parseUpdateData(tableId, buffer);
            }
            
            String tupleType = new String(new byte[]{buffer.get()}, 0, 1);
            switch (tupleType) {
                case "N":
                case "K":
                case "O":
                    List<Object> data = new ArrayList<>();
                    readTupleData(tableId, buffer, data);
                    return new RowChangedEvent(tableId.tableName, type.name(), data, null, null);

                default:
                    logger.info("N, K, O not set, got instead {}", tupleType);
            }
        }
        return null;
    }
    
    /**
     * 解析 UPDATE 数据，处理新旧值合并
     * PostgreSQL pgoutput 在 REPLICA IDENTITY FULL 模式下会发送:
     * 1. "O" - 旧值元组 (可选)
     * 2. "N" - 新值元组
     * 
     * 新值中可能包含 TOASTED ("u") 字段，表示该字段未变更，需要从旧值中获取
     */
    private RowChangedEvent parseUpdateData(TableId tableId, ByteBuffer buffer) {
        List<Object> oldData = null;
        List<Object> newData = null;
        
        String firstTupleType = new String(new byte[]{buffer.get()}, 0, 1);
        
        // 如果第一个是旧值 "O"，先读取旧值
        if ("O".equals(firstTupleType)) {
            oldData = new ArrayList<>();
            readTupleData(tableId, buffer, oldData);
            
            // 继续读取新值标识符
            if (buffer.hasRemaining()) {
                String secondTupleType = new String(new byte[]{buffer.get()}, 0, 1);
                if ("N".equals(secondTupleType)) {
                    newData = new ArrayList<>();
                    readTupleData(tableId, buffer, newData);
                }
            }
        } else if ("N".equals(firstTupleType)) {
            // 直接读取新值（没有旧值的情况）
            newData = new ArrayList<>();
            readTupleData(tableId, buffer, newData);
        } else {
            // 这里原则上不会进入，但不排除日志格式变更，故严谨起见增加日志输出，便于外部感知
            logger.error("UPDATE: unexpected tuple type '{}', expected 'O' or 'N'", firstTupleType);
            return null;
        }
        
        // 合并新旧值：如果新值中有 TOASTED 字段，使用旧值填充
        List<Object> mergedData = mergeUpdateData(oldData, newData);
        return new RowChangedEvent(tableId.tableName, MessageTypeEnum.UPDATE.name(), mergedData, null, null);
    }
    
    /**
     * 合并 UPDATE 的新旧值
     * 如果新值中某些字段是 TOASTED（未变更），则使用旧值中的对应字段
     */
    private List<Object> mergeUpdateData(List<Object> oldData, List<Object> newData) {
        if (newData == null) {
            return oldData != null ? oldData : new ArrayList<>();
        }
        
        if (oldData == null) {
            return newData;
        }
        
        // 合并数据：新值中的 TOASTED 字段用旧值替换
        List<Object> merged = new ArrayList<>(newData.size());
        for (int i = 0; i < newData.size(); i++) {
            Object newValue = newData.get(i);
            if ("TOASTED".equals(newValue) && i < oldData.size()) {
                // 使用旧值填充 TOASTED 字段
                merged.add(oldData.get(i));
            } else {
                merged.add(newValue);
            }
        }
        return merged;
    }

    private void readTupleData(TableId tableId, ByteBuffer msg, List<Object> data) {
        short nColumn = msg.getShort();
        if (nColumn != tableId.fields.size()) {
            logger.warn("The column size of table '{}' is {}, but we has been received column size is {}.", tableId.tableName, tableId.fields.size(), nColumn);

            // The table schema has been changed, we should be get a new table schema from db.
            MetaInfo metaInfo = getMetaInfo(tableId.tableName);
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

    private MetaInfo getMetaInfo(String tableName) {
        DefaultConnectorServiceContext context = new DefaultConnectorServiceContext();
        context.setCatalog(database);
        context.setSchema(schema);
        context.addTablePattern(tableName);
        List<MetaInfo> metaInfos = connectorService.getMetaInfo(connectorInstance, context);
        MetaInfo metaInfo = CollectionUtils.isEmpty(metaInfos) ? null : metaInfos.get(0);
        Assert.isTrue(metaInfo != null, String.format("The table '%s' is not exist in schema '%s'.", tableName, schema));
        // 添加详细日志，方便诊断问题
        if (CollectionUtils.isEmpty(metaInfo.getColumn())) {
            logger.error("Table '{}.{}' has no columns. This may be caused by unsupported column types. ", schema, tableName);
            throw new IllegalArgumentException(
                    String.format("The table column for '%s.%s' must not be empty. Please check table structure and ensure all column types are supported.", schema, tableName));
        }
        return metaInfo;
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