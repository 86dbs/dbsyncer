/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mongodb.cdc.MongoDBChangeStreamListener;
import org.dbsyncer.connector.mongodb.cdc.MongoDBQuartzListener;
import org.dbsyncer.connector.mongodb.config.MongoDBConfig;
import org.dbsyncer.connector.mongodb.constant.MongoDBConstant;
import org.dbsyncer.connector.mongodb.schema.MongoDBSchemaResolver;
import org.dbsyncer.connector.mongodb.util.MongoUtil;
import org.dbsyncer.connector.mongodb.validator.MongoDBConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.DefaultMetaContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MongoDB 连接器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public final class MongoDBConnector extends AbstractConnector implements ConnectorService<MongoConnectorInstance, MongoDBConfig> {

    public static final String SOURCE_COLLECTION = MongoDBConstant.SOURCE_COLLECTION;
    public static final String TARGET_COLLECTION = MongoDBConstant.TARGET_COLLECTION;
    public static final String DATABASE = MongoDBConstant.DATABASE;

    private static final Set<String> SYSTEM_DATABASES = new LinkedHashSet<String>() {{
        add("admin");
        add("config");
        add("local");
    }};

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MongoDBConfigValidator configValidator = new MongoDBConfigValidator();
    private final MongoDBSchemaResolver schemaResolver = new MongoDBSchemaResolver();

    @Override
    public String getConnectorType() {
        return "MongoDB";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
    }

    @Override
    public Class<MongoDBConfig> getConfigClass() {
        return MongoDBConfig.class;
    }

    @Override
    public ConnectorInstance connect(MongoDBConfig config, ConnectorServiceContext context) {
        return new MongoConnectorInstance(config);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public void disconnect(MongoConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(MongoConnectorInstance connectorInstance) {
        try {
            connectorInstance.getConnection().getDatabase("admin")
                    .runCommand(new Document("ping", 1));
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new MongoDBException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getDatabases(MongoConnectorInstance connectorInstance) {
        List<String> databases = new ArrayList<>();
        for (String name : connectorInstance.getConnection().listDatabaseNames()) {
            if (!SYSTEM_DATABASES.contains(name)) {
                databases.add(name);
            }
        }
        return databases;
    }

    @Override
    public List<Table> getTable(MongoConnectorInstance connectorInstance, ConnectorServiceContext context) {
        String databaseName = resolveDatabase(connectorInstance, context);
        if (StringUtil.isBlank(databaseName)) {
            return Collections.emptyList();
        }
        MongoDatabase database = connectorInstance.getConnection().getDatabase(databaseName);
        List<Table> tables = new ArrayList<>();
        for (String collectionName : database.listCollectionNames()) {
            if (StringUtil.startsWith(collectionName, "system.")) {
                continue;
            }
            Table table = new Table();
            table.setName(collectionName);
            table.setType(TableTypeEnum.TABLE.getCode());
            tables.add(table);
        }
        return tables;
    }

    @Override
    public List<MetaInfo> getMetaInfo(MongoConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<MetaInfo> metaInfos = new ArrayList<>();
        String databaseName = resolveDatabase(connectorInstance, context);
        if (StringUtil.isBlank(databaseName)) {
            return metaInfos;
        }
        MongoDatabase database = connectorInstance.getConnection().getDatabase(databaseName);
        for (Table table : context.getTablePatterns()) {
            if (TableTypeEnum.getTableType(table.getType()) == getExtendedTableType()) {
                MetaInfo metaInfo = new MetaInfo();
                metaInfo.setTable(table.getName());
                metaInfo.setTableType(table.getType());
                metaInfo.setColumn(table.getColumn());
                metaInfo.setExtInfo(table.getExtInfo());
                metaInfos.add(metaInfo);
                continue;
            }
            MongoCollection<Document> collection = database.getCollection(table.getName());
            Document sample = collection.find().limit(1).first();
            List<Field> fields = buildFields(sample);
            MetaInfo metaInfo = new MetaInfo();
            metaInfo.setTable(table.getName());
            metaInfo.setTableType(table.getType());
            metaInfo.setColumn(fields);
            metaInfos.add(metaInfo);
        }
        return metaInfos;
    }

    @Override
    public long getCount(MongoConnectorInstance connectorInstance, MetaContext metaContext) {
        Map<String, String> command = metaContext.getCommand();
        String databaseName = command.get(DATABASE);
        String collectionName = command.get(SOURCE_COLLECTION);
        if (metaContext instanceof DefaultMetaContext && ((DefaultMetaContext) metaContext).isTargetConnector()) {
            collectionName = command.get(ConnectorConstant.TARGET_QUERY_COUNT);
        }
        if (StringUtil.isBlank(databaseName) || StringUtil.isBlank(collectionName)) {
            return 0L;
        }
        return connectorInstance.getConnection().getDatabase(databaseName)
                .getCollection(collectionName)
                .countDocuments();
    }

    @Override
    public Result reader(MongoConnectorInstance connectorInstance, ReaderContext context) {
        Map<String, String> command = context.getCommand();
        String databaseName = command.get(DATABASE);
        String collectionName = command.get(SOURCE_COLLECTION);
        if (StringUtil.isBlank(databaseName) || StringUtil.isBlank(collectionName)) {
            throw new MongoDBException("MongoDB reader database or collection is empty.");
        }
        MongoCollection<Document> collection = connectorInstance.getConnection()
                .getDatabase(databaseName)
                .getCollection(collectionName);
        String primaryKey = resolvePrimaryKey(context.getSourceTable());
        Bson filter = buildCursorFilter(primaryKey, context.getCursors());
        int pageSize = context.getPageSize();
        List<Map<String, Object>> list = new ArrayList<>(pageSize);
        try (MongoCursor<Document> cursor = collection.find(filter)
                .sort(Sorts.ascending(primaryKey))
                .projection(buildProjection(command.get(ConnectorConstant.OPERTION_QUERY)))
                .skip(resolveSkip(context))
                .limit(pageSize)
                .iterator()) {
            while (cursor.hasNext()) {
                list.add(MongoUtil.toMap(cursor.next()));
            }
        }
        return new Result(list);
    }

    @Override
    public Result writer(MongoConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            throw new MongoDBException("writer data can not be empty.");
        }
        Result result = new Result();
        String databaseName = context.getCommand().get(DATABASE);
        String collectionName = context.getCommand().get(TARGET_COLLECTION);
        if (StringUtil.isBlank(databaseName) || StringUtil.isBlank(collectionName)) {
            throw new MongoDBException("MongoDB writer database or collection is empty.");
        }
        MongoCollection<Document> collection = connectorInstance.getConnection()
                .getDatabase(databaseName)
                .getCollection(collectionName);
        List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        String event = context.getEvent();
        ReplaceOptions upsertOptions = new ReplaceOptions().upsert(true);
        for (Map row : data) {
            try {
                Document document = MongoUtil.toDocument(row);
                if (StringUtil.equals(event, ConnectorConstant.OPERTION_DELETE)) {
                    Bson filter = buildPkFilter(pkFields, row);
                    if (collection.deleteOne(filter).getDeletedCount() == 0) {
                        throw new MongoDBException("删除数据不存在");
                    }
                } else if (StringUtil.equals(event, ConnectorConstant.OPERTION_INSERT)) {
                    collection.insertOne(document);
                } else if (StringUtil.equals(event, ConnectorConstant.OPERTION_UPDATE)) {
                    Bson filter = buildPkFilter(pkFields, row);
                    if (collection.replaceOne(filter, document).getMatchedCount() == 0) {
                        throw new MongoDBException("更新数据不存在");
                    }
                } else {
                    collection.replaceOne(buildPkFilter(pkFields, row), document, upsertOptions);
                }
                result.getSuccessData().add(row);
            } catch (Exception e) {
                result.getFailData().add(row);
                result.getError().append(e.getMessage()).append(System.lineSeparator());
                logger.error(e.getMessage(), e);
            }
        }
        return result;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        Map<String, String> command = new HashMap<>();
        Table table = commandConfig.getTable();
        command.put(SOURCE_COLLECTION, table.getName());
        command.put(DATABASE, resolveDatabaseName(commandConfig));
        if (!CollectionUtils.isEmpty(table.getColumn())) {
            List<String> fieldNames = table.getColumn().stream().map(Field::getName).collect(Collectors.toList());
            command.put(ConnectorConstant.OPERTION_QUERY, StringUtil.join(fieldNames, StringUtil.COMMA));
        }
        if (!CollectionUtils.isEmpty(commandConfig.getFilter())) {
            command.put(ConnectorConstant.OPERTION_QUERY_FILTER, JsonUtil.objToJson(commandConfig.getFilter()));
        }
        return command;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        PrimaryKeyUtil.findTablePrimaryKeys(table);
        Map<String, String> command = new HashMap<>();
        command.put(TARGET_COLLECTION, table.getName());
        command.put(DATABASE, resolveDatabaseName(commandConfig));
        command.put(ConnectorConstant.TARGET_QUERY_COUNT, table.getName());
        command.put(ConnectorConstant.OPERTION_QUERY_TARGET, table.getName());
        return command;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new MongoDBQuartzListener();
        }
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new MongoDBChangeStreamListener();
        }
        return null;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    private List<Field> buildFields(Document sample) {
        Set<String> fieldNames = new LinkedHashSet<>();
        fieldNames.add(MongoDBConstant.ID_FIELD);
        if (sample != null) {
            fieldNames.addAll(sample.keySet());
        }
        List<Field> fields = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            Object value = sample == null ? null : sample.get(fieldName);
            String typeName = MongoUtil.inferTypeName(fieldName, value);
            Field field = new Field(fieldName, typeName, 0, MongoDBConstant.ID_FIELD.equals(fieldName));
            fields.add(field);
        }
        return fields;
    }

    private String resolveDatabase(MongoConnectorInstance connectorInstance, ConnectorServiceContext context) {
        if (context != null && StringUtil.isNotBlank(context.getCatalog())) {
            return context.getCatalog().trim();
        }
        return connectorInstance.getConfig().getDatabase();
    }

    private String resolveDatabaseName(CommandConfig commandConfig) {
        if (commandConfig != null && StringUtil.isNotBlank(commandConfig.getSchema())) {
            return commandConfig.getSchema().trim();
        }
        if (commandConfig != null && commandConfig.getConnectorConfig() instanceof MongoDBConfig) {
            return ((MongoDBConfig) commandConfig.getConnectorConfig()).getDatabase();
        }
        return null;
    }

    private String resolvePrimaryKey(Table table) {
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return MongoDBConstant.ID_FIELD;
        }
        return primaryKeys.get(0);
    }

    private int resolveSkip(ReaderContext context) {
        Object[] cursors = context.getCursors();
        if (cursors != null && cursors.length > 0 && cursors[0] != null) {
            return 0;
        }
        return Math.max(0, (context.getPageIndex() - 1) * context.getPageSize());
    }

    private Bson buildCursorFilter(String primaryKey, Object[] cursors) {
        if (cursors == null || cursors.length == 0 || cursors[0] == null) {
            return new Document();
        }
        return Filters.gt(primaryKey, MongoUtil.normalizeId(cursors[0]));
    }

    private Bson buildProjection(String fields) {
        if (StringUtil.isBlank(fields)) {
            return null;
        }
        Document projection = new Document();
        for (String field : fields.split(StringUtil.COMMA)) {
            if (StringUtil.isNotBlank(field)) {
                projection.append(field.trim(), 1);
            }
        }
        return projection.isEmpty() ? null : projection;
    }

    private Bson buildPkFilter(List<Field> pkFields, Map row) {
        if (CollectionUtils.isEmpty(pkFields)) {
            return Filters.eq(MongoDBConstant.ID_FIELD, MongoUtil.normalizeId(row.get(MongoDBConstant.ID_FIELD)));
        }
        if (pkFields.size() == 1) {
            Field field = pkFields.get(0);
            return Filters.eq(field.getName(), MongoUtil.normalizeWriteValue(field.getName(), row.get(field.getName())));
        }
        List<Bson> filters = new ArrayList<>(pkFields.size());
        for (Field field : pkFields) {
            filters.add(Filters.eq(field.getName(), MongoUtil.normalizeWriteValue(field.getName(), row.get(field.getName()))));
        }
        return Filters.and(filters);
    }
}
