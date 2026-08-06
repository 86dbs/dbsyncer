/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.connector.starrocks.constant.StarRocksConstant;
import org.dbsyncer.connector.starrocks.load.StarRocksStreamLoadWriter;
import org.dbsyncer.connector.starrocks.schema.StarRocksSchemaResolver;
import org.dbsyncer.connector.starrocks.validator.StarRocksConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * StarRocks 连接器（兼容 MySQL 协议）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksConnector extends MySQLConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
            "information_schema", "sys", "_statistics_")
            .collect(Collectors.toSet());

    private final StarRocksConfigValidator configValidator = new StarRocksConfigValidator();
    private final StarRocksSchemaResolver schemaResolver = new StarRocksSchemaResolver();
    private final StarRocksStreamLoadWriter streamLoadWriter = new StarRocksStreamLoadWriter();

    @Override
    public String getConnectorType() {
        return "StarRocks";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        StarRocksConstant.enrichJdbcProperties(config);
        return super.connect(config, context);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        DatabaseConfig config = connectorInstance.getConfig();
        if (StarRocksConstant.isStreamLoadMode(config) && StarRocksConstant.isStreamLoadEvent(context.getEvent())) {
            return streamLoadWriter.write(connectorInstance, context);
        }
        return super.writer(connectorInstance, context);
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    /**
     * StarRocks 不支持 MySQL 的 {@code INSERT IGNORE} 语法，主键模型（Primary Key / Unique Key）表
     * 通过 {@code INSERT INTO} 即可按主键自动去重覆盖，故这里改为纯 {@code INSERT INTO}。
     */
    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        return buildInsertIntoSql(config);
    }

    /**
     * StarRocks 不支持 MySQL 的 {@code ON DUPLICATE KEY UPDATE} 语法，主键模型表的 {@code INSERT INTO}
     * 本身即为 UPSERT（按主键覆盖），故 upsert 与 insert 使用相同的 {@code INSERT INTO} 语句。
     */
    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        return buildInsertIntoSql(config);
    }

    private String buildInsertIntoSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        fields.forEach(f -> {
            fs.add(database.buildWithQuotation(f.getName()));
            vs.add("?");
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(database.buildWithQuotation(config.getTableName()));
        String fieldNames = StringUtil.join(fs, StringUtil.COMMA);
        String values = StringUtil.join(vs, StringUtil.COMMA);
        return String.format("%sINSERT INTO %s (%s) VALUES (%s)", uniqueCode, table, fieldNames, values);
    }

    @Override
    public StorageService getStorageService() {
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (CollectionUtils.isEmpty(databases)) {
                return Collections.emptyList();
            }
            return databases.stream()
                    .filter(name -> !SYSTEM_DATABASES.contains(name.toLowerCase()))
                    .collect(Collectors.toList());
        });
    }

    /**
     * StarRocks 的 JDBC {@code getPrimaryKeys} 对 key 模型表通常返回空，导致列元数据缺少主键标记，
     * 整库迁移时会因「缺少主键」失败。故列类型仍走父类 JDBC，主键改用 {@code DESC table} 的 Key 列
     *（key 模型排序键为 true）补齐。
     */
    @Override
    public List<MetaInfo> getMetaInfo(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        if (CollectionUtils.isEmpty(context.getTablePatterns())) {
            return Collections.emptyList();
        }
        for (Table table : context.getTablePatterns()) {
            if (TableTypeEnum.getTableType(table.getType()) == getExtendedTableType()) {
                return super.getMetaInfo(connectorInstance, context);
            }
        }
        List<MetaInfo> metaInfos = super.getMetaInfo(connectorInstance, context);
        if (CollectionUtils.isEmpty(metaInfos)) {
            return metaInfos;
        }
        String database = StringUtil.getIfBlank(context.getCatalog(), context.getSchema());
        return connectorInstance.execute(databaseTemplate -> {
            for (MetaInfo metaInfo : metaInfos) {
                if (metaInfo == null || StringUtil.isBlank(metaInfo.getTable()) || CollectionUtils.isEmpty(metaInfo.getColumn())) {
                    continue;
                }
                try {
                    String table = buildWithQuotation(metaInfo.getTable());
                    if (StringUtil.isNotBlank(database)) {
                        table = buildWithQuotation(database) + "." + table;
                    }
                    // queryForList 返回大小写不敏感的 LinkedCaseInsensitiveMap，可直接按 Field/Key 取值
                    Set<String> keys = new HashSet<>();
                    for (Map<String, Object> row : databaseTemplate.queryForList("DESC " + table)) {
                        if (isKeyColumn(row.get("Key"))) {
                            keys.add(String.valueOf(row.get("Field")).toUpperCase(Locale.ROOT));
                        }
                    }
                    metaInfo.getColumn().forEach(f -> f.setPk(keys.contains(f.getName().toUpperCase(Locale.ROOT))));
                } catch (Exception e) {
                    // 键标记失败不阻断元数据加载，下游无主键时再由业务报错
                    logger.warn("获取 StarRocks 键列失败, database={}, table={}, err={}", database, metaInfo.getTable(), e.getMessage());
                }
            }
            return metaInfos;
        });
    }

    /**
     * StarRocks {@code DESC} 的 Key 列在 key 模型排序键为 true；兼容 PRI/UNI/DUP（部分版本/明细模型排序键）。
     */
    private static boolean isKeyColumn(Object columnKey) {
        if (columnKey == null) {
            return false;
        }
        String key = columnKey.toString().trim();
        return "true".equalsIgnoreCase(key) || "PRI".equalsIgnoreCase(key)
                || "UNI".equalsIgnoreCase(key) || "DUP".equalsIgnoreCase(key);
    }
}
