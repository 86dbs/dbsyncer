/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.dameng.cdc.DamengListener;
import org.dbsyncer.connector.dameng.constant.DamengConstant;
import org.dbsyncer.connector.dameng.schema.DamengSchemaResolver;
import org.dbsyncer.connector.dameng.validator.DamengConfigValidator;
import org.dbsyncer.connector.oracle.OracleConnector;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseTemplate;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.schema.BindParameter;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 达梦（DM）连接器（兼容 Oracle 语法）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 02:00
 */
public final class DamengConnector extends OracleConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnector.class);

    private static final String QUERY_SCHEMA = "SELECT USERNAME FROM DBA_USERS WHERE USERNAME NOT IN "
            + "('SYS','SYSSSO','SYSAUDITOR','SYSDBO','CTISYS') ORDER BY USERNAME";

    /**
     * 判断表是否含 IDENTITY 自增列（SYSCOLUMNS.INFO2 bit0）。
     */
    private static final String QUERY_TABLE_IDENTITY =
            "SELECT COUNT(1) FROM SYSCOLUMNS COL, SYSOBJECTS TAB "
                    + "WHERE COL.ID = TAB.ID AND TAB.SUBTYPE$ = 'UTAB' AND BITAND(COL.INFO2, 1) = 1 AND TAB.NAME = ?";

    private static final String QUERY_TABLE_IDENTITY_WITH_SCHEMA =
            "SELECT COUNT(1) FROM SYSCOLUMNS COL, SYSOBJECTS TAB, SYSOBJECTS SCH "
                    + "WHERE COL.ID = TAB.ID AND TAB.SCHID = SCH.ID AND TAB.SUBTYPE$ = 'UTAB' "
                    + "AND BITAND(COL.INFO2, 1) = 1 AND TAB.NAME = ? AND SCH.NAME = ?";

    /**
     * getTargetCommand 包装后的 SQL：SET IDENTITY_INSERT ... ON;{merge};SET IDENTITY_INSERT ... OFF
     */
    private static final Pattern IDENTITY_WRAP = Pattern.compile(
            "^SET IDENTITY_INSERT (.+?) ON;(.+);SET IDENTITY_INSERT \\1 OFF$",
            Pattern.DOTALL);

    private final DamengConfigValidator configValidator = new DamengConfigValidator();
    private final DamengSchemaResolver schemaResolver = new DamengSchemaResolver();

    @Override
    public String getConnectorType() {
        return "Dameng";
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
        DamengConstant.enrichJdbcProperties(config);
        // 纠正历史错误 URL（曾把库名拼到路径上，驱动会当作 schema）
        config.setUrl(buildJdbcUrl(config, config.getDatabase()));
        return super.connect(config, context);
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DamengListener();
        }
        return null;
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
    }

    /**
     * 达梦 JDBC URL 为 {@code jdbc:dm://host:port}；路径上的名称会被驱动当作<strong>模式名</strong>，
     * 不能把实例库名（如 DAMENG）拼进去。默认模式与用户名一致，如需指定请用连接参数 {@code schema=}。
     */
    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        return "jdbc:dm://" + config.getHost() + ":" + config.getPort();
    }

    /**
     * 含 IDENTITY 自增列时，全量/覆盖写入需临时打开 IDENTITY_INSERT，否则报 -2723。
     */
    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> targetCommand = super.getTargetCommand(commandConfig);
        if (!hasIdentityColumn(commandConfig)) {
            return targetCommand;
        }
        String qualified = qualifyIdentityTable(commandConfig.getSchema(), commandConfig.getTable().getName());
        wrapIdentityInsert(targetCommand, ConnectorConstant.OPERTION_INSERT, qualified);
        wrapIdentityInsert(targetCommand, ConnectorConstant.OPERTION_UPSERT, qualified);
        return targetCommand;
    }

    /**
     * 达梦 MERGE 成功时常返回 0（非 MySQL 的 1/2），需计入成功，否则进度 success 一直为 0。
     */
    @Override
    protected boolean isWriteSuccess(int affectedRows, String event) {
        return affectedRows >= 0 || affectedRows == -2;
    }

    @Override
    protected String resolveExecuteSql(String executeSql) {
        Matcher matcher = IDENTITY_WRAP.matcher(StringUtil.trimToEmpty(executeSql));
        return matcher.matches() ? matcher.group(2) : executeSql;
    }

    /**
     * 达梦 PreparedStatement 对多语句支持不稳定，将 IDENTITY_INSERT 与 MERGE 拆开执行。
     */
    @Override
    protected int[] batchUpdate(DatabaseTemplate databaseTemplate, String executeSql, List<Field> fields, List<Map> data) throws Exception {
        Matcher matcher = IDENTITY_WRAP.matcher(StringUtil.trimToEmpty(executeSql));
        if (!matcher.matches()) {
            return normalizeDamengBatchResult(super.batchUpdate(databaseTemplate, executeSql, fields, data));
        }
        String qualified = matcher.group(1);
        String mergeSql = matcher.group(2);
        String onSql = "SET IDENTITY_INSERT " + qualified + " ON";
        String offSql = "SET IDENTITY_INSERT " + qualified + " OFF";
        SimpleConnection connection = databaseTemplate.getSimpleConnection();
        try {
            connection.setAutoCommit(false);
            databaseTemplate.execute(onSql);
            int[] result = databaseTemplate.batchUpdate(mergeSql, batchRows(fields, data));
            databaseTemplate.execute(offSql);
            connection.commit();
            return normalizeDamengBatchResult(result);
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            try {
                databaseTemplate.execute(offSql);
            } catch (Exception ignore) {
                // 会话结束时会自动还原 OFF
            }
            connection.setAutoCommit(true);
        }
    }

    /**
     * 将达梦 MERGE 返回的 0 规范为 SUCCESS_NO_INFO(-2)，避免被当成失败。
     */
    private static int[] normalizeDamengBatchResult(int[] result) {
        if (result == null) {
            return null;
        }
        for (int i = 0; i < result.length; i++) {
            if (result[i] == 0) {
                result[i] = -2;
            }
        }
        return result;
    }

    /**
     * 逐条降级时也要先开 IDENTITY_INSERT，再执行纯 MERGE。
     */
    @Override
    protected void forceUpdate(DatabaseConnectorInstance connectorInstance, PluginContext context,
                               String executeSql, List<Field> fields, String event, List<Map> data, Result result) {
        Matcher matcher = IDENTITY_WRAP.matcher(StringUtil.trimToEmpty(executeSql));
        if (!matcher.matches()) {
            super.forceUpdate(connectorInstance, context, executeSql, fields, event, data, result);
            return;
        }
        String qualified = matcher.group(1);
        String mergeSql = matcher.group(2);
        String onSql = "SET IDENTITY_INSERT " + qualified + " ON";
        String offSql = "SET IDENTITY_INSERT " + qualified + " OFF";
        try {
            connectorInstance.execute(databaseTemplate -> {
                databaseTemplate.execute(onSql);
                return null;
            });
            super.forceUpdate(connectorInstance, context, mergeSql, fields, event, data, result);
        } finally {
            try {
                connectorInstance.execute(databaseTemplate -> {
                    databaseTemplate.execute(offSql);
                    return null;
                });
            } catch (Exception ignore) {
                // ignore
            }
        }
    }

    /**
     * MERGE INTO ... USING (SELECT ? FROM DUAL) 时，达梦对大字段默认按 VARCHAR 推断，
     * 需显式 CAST，否则报「无法转换的数据类型」。
     */
    @Override
    public boolean buildCustomValue(List<String> vs, Field field) {
        String type = DamengSchemaResolver.normalizeTypeName(field == null ? null : field.getTypeName());
        if (isClobFamily(type)) {
            vs.add("CAST(? AS CLOB)");
            return true;
        }
        if (isBlobFamily(type)) {
            vs.add("CAST(? AS BLOB)");
            return true;
        }
        return false;
    }

    /**
     * 二进制列使用 createBlob/setBytes，避免 setObject 推断失败。
     */
    @Override
    protected Object wrapBindParameter(Field field, Object val) {
        if (!(val instanceof byte[])) {
            return val;
        }
        final byte[] bytes = (byte[]) val;
        final String type = DamengSchemaResolver.normalizeTypeName(field == null ? null : field.getTypeName());
        if (isBlobFamily(type)) {
            return (BindParameter) (ps, paramIndex, connection) -> {
                if (bytes.length == 0) {
                    ps.setBytes(paramIndex, bytes);
                    return;
                }
                Blob blob = connection.createBlob();
                blob.setBytes(1, bytes);
                ps.setBlob(paramIndex, blob);
            };
        }
        return (BindParameter) (ps, paramIndex, connection) -> ps.setBytes(paramIndex, bytes);
    }

    /**
     * MySQL → 达梦类型映射，避免同构建表时残留 MySQL 专有类型名。
     */
    @Override
    public String formatPhysicalType(Field sourceDefinition) {
        String raw = sourceDefinition == null ? null : sourceDefinition.getTypeName();
        if (StringUtil.isBlank(raw)) {
            return "VARCHAR(255)";
        }
        String t = raw.trim().toUpperCase(Locale.ROOT);
        if ("YEAR".equals(t)) {
            return "INT";
        }
        if ("MEDIUMINT".equals(t) || "MEDIUMINT UNSIGNED".equals(t) || "INT UNSIGNED".equals(t)) {
            return "INT";
        }
        if ("TINYINT UNSIGNED".equals(t) || "SMALLINT UNSIGNED".equals(t)) {
            return "SMALLINT";
        }
        if ("BIGINT UNSIGNED".equals(t)) {
            return "BIGINT";
        }
        if ("JSON".equals(t) || t.endsWith("TEXT")) {
            return "TEXT";
        }
        if (t.endsWith("BLOB") || "IMAGE".equals(t) || "LONGVARBINARY".equals(t)) {
            return "BLOB";
        }
        if (t.endsWith(" UNSIGNED")) {
            return formatPhysicalType(copyWithoutUnsigned(sourceDefinition, t));
        }
        return super.formatPhysicalType(sourceDefinition);
    }

    private boolean hasIdentityColumn(CommandConfig commandConfig) {
        String tableName = commandConfig.getTable() == null ? null : commandConfig.getTable().getName();
        if (StringUtil.isBlank(tableName)) {
            return false;
        }
        DatabaseConnectorInstance db = (DatabaseConnectorInstance) commandConfig.getConnectorInstance();
        if (db == null) {
            return false;
        }
        try {
            String schema = commandConfig.getSchema();
            Integer count = db.execute(databaseTemplate -> {
                if (StringUtil.isNotBlank(schema)) {
                    return databaseTemplate.queryForObject(QUERY_TABLE_IDENTITY_WITH_SCHEMA, Integer.class, tableName, schema);
                }
                return databaseTemplate.queryForObject(QUERY_TABLE_IDENTITY, Integer.class, tableName);
            });
            return count != null && count > 0;
        } catch (Exception e) {
            LOGGER.warn("Detect Dameng identity column failed, table={}: {}", tableName, e.getMessage());
            return false;
        }
    }

    private void wrapIdentityInsert(Map<String, String> targetCommand, String operation, String qualifiedTable) {
        String sql = targetCommand.get(operation);
        if (StringUtil.isBlank(sql)) {
            return;
        }
        targetCommand.put(operation, "SET IDENTITY_INSERT " + qualifiedTable + " ON;" + sql
                + ";SET IDENTITY_INSERT " + qualifiedTable + " OFF");
    }

    private String qualifyIdentityTable(String schema, String tableName) {
        if (StringUtil.isNotBlank(schema)) {
            return buildWithQuotation(schema) + "." + buildWithQuotation(tableName);
        }
        return buildWithQuotation(tableName);
    }

    private static Field copyWithoutUnsigned(Field source, String typeName) {
        String base = typeName.substring(0, typeName.length() - " UNSIGNED".length());
        return new Field(source.getName(), base, source.getType(), source.isPk(), source.getColumnSize(), source.getRatio());
    }

    private static boolean isClobFamily(String type) {
        if (StringUtil.isBlank(type)) {
            return false;
        }
        return type.contains("TEXT")
                || type.contains("CLOB")
                || "LONGVARCHAR".equals(type)
                || "LONG".equals(type)
                || "JSON".equals(type);
    }

    private static boolean isBlobFamily(String type) {
        if (StringUtil.isBlank(type)) {
            return false;
        }
        return type.contains("BLOB")
                || "IMAGE".equals(type)
                || "LONGVARBINARY".equals(type)
                || "LONG RAW".equals(type);
    }
}
