/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbaseoracle;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbaseoracle.schema.OceanBaseOracleSchemaResolver;
import org.dbsyncer.connector.oceanbaseoracle.validator.OceanBaseOracleConfigValidator;
import org.dbsyncer.connector.oracle.OracleConnector;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseTemplate;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OceanBase Oracle 兼容模式连接器。
 * <p>复用 Oracle 方言（双引号、MERGE、Schema 命名空间）；驱动与 URL 使用 oceanbase-client。
 * 第一期支持全量与定时增量，暂不支持 Log CDC。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 14:10
 */
public final class OceanBaseOracleConnector extends OracleConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(OceanBaseOracleConnector.class);

    private static final String QUERY_SCHEMA = "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME";

    /**
     * oceanbase-client 的 DatabaseMetaData#getTables 在 Oracle 模式下常返回空
     * （tableNamePattern=null / 类型过滤不兼容），改为查数据字典。
     */
    private static final String QUERY_TABLES =
            "SELECT TABLE_NAME AS OBJECT_NAME, 'TABLE' AS OBJECT_TYPE FROM ALL_TABLES WHERE OWNER = ? "
                    + "UNION ALL "
                    + "SELECT VIEW_NAME AS OBJECT_NAME, 'VIEW' AS OBJECT_TYPE FROM ALL_VIEWS WHERE OWNER = ?";

    private static final String QUERY_MVIEWS =
            "SELECT MVIEW_NAME AS OBJECT_NAME, 'MATERIALIZED VIEW' AS OBJECT_TYPE FROM ALL_MVIEWS WHERE OWNER = ?";

    private static final String QUERY_COMPAT_MODE_V =
            "SELECT VALUE FROM V$OB_PARAMETERS WHERE NAME = 'ob_compatibility_mode'";

    private static final String QUERY_COMPAT_MODE_SHOW =
            "SHOW VARIABLES LIKE 'ob_compatibility_mode'";

    private static final String EXPECTED_MODE = "ORACLE";

    private final OceanBaseOracleConfigValidator configValidator = new OceanBaseOracleConfigValidator();
    private final OceanBaseOracleSchemaResolver schemaResolver = new OceanBaseOracleSchemaResolver();

    private final Set<String> SYSTEM_SCHEMAS = Stream.of(
                    "ANONYMOUS", "APPQOSSYS", "AUDSYS", "CTXSYS", "DBSFWUSER", "DBSNMP", "DVSYS",
                    "GSMADMIN_INTERNAL", "LBACSYS", "MDSYS", "OJVMSYS", "OLAPSYS",
                    "ORDDATA", "ORDSYS", "ORAAUDITOR", "OUTLN", "SYS", "SYSTEM",
                    "SYS_EXTERNAL_TBS", "WMSYS", "XDB", "XS$NULL",
                    "OCEANBASE", "__PUBLIC", "__RECYCLEBIN", "OCS")
            .collect(Collectors.toSet());

    @Override
    public String getConnectorType() {
        return "OceanBaseOracle";
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
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        // Oracle 模式租户不适用 MySQL Binlog / Oracle LogMiner，首期不支持日志增量
        if (ListenerTypeEnum.isLog(listenerType)) {
            return null;
        }
        return null;
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        String currentUser = resolveCurrentUser(connectorInstance);
        return connectorInstance.execute(databaseTemplate -> {
            List<String> schemas;
            try {
                schemas = databaseTemplate.queryForList(QUERY_SCHEMA, String.class);
            } catch (Exception e) {
                LOGGER.warn("Query ALL_USERS failed, fallback to current user: {}", e.getMessage());
                schemas = Collections.emptyList();
            }
            List<String> result = CollectionUtils.isEmpty(schemas)
                    ? new ArrayList<>()
                    : schemas.stream()
                    .filter(name -> name != null && !SYSTEM_SCHEMAS.contains(name.toUpperCase(Locale.ROOT)))
                    .collect(Collectors.toCollection(ArrayList::new));
            // OceanBase Oracle 租户常用 SYS@tenant 登录；SYS 在系统名单中会被过滤，需始终保留当前登录用户
            ensureCurrentUserSchema(result, currentUser);
            if (result.isEmpty() && StringUtil.isNotBlank(currentUser)) {
                result.add(currentUser);
            }
            return result;
        });
    }

    /**
     * 按 Schema(OWNER) 列出物理表/视图；不走 JDBC DatabaseMetaData，避免 oceanbase-client 空结果。
     */
    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        String schema = context == null ? null : context.getSchema();
        if (StringUtil.isBlank(schema)) {
            schema = resolveCurrentUser(connectorInstance);
        }
        if (StringUtil.isBlank(schema)) {
            return Collections.emptyList();
        }
        final String owner = schema.trim().toUpperCase(Locale.ROOT);
        return connectorInstance.execute(databaseTemplate -> {
            List<Map<String, Object>> rows = new ArrayList<>();
            List<Map<String, Object>> baseRows = databaseTemplate.queryForList(QUERY_TABLES, owner, owner);
            if (!CollectionUtils.isEmpty(baseRows)) {
                rows.addAll(baseRows);
            }
            try {
                List<Map<String, Object>> mviewRows = databaseTemplate.queryForList(QUERY_MVIEWS, owner);
                if (!CollectionUtils.isEmpty(mviewRows)) {
                    rows.addAll(mviewRows);
                }
            } catch (Exception e) {
                LOGGER.debug("Query ALL_MVIEWS skipped: {}", e.getMessage());
            }
            if (CollectionUtils.isEmpty(rows)) {
                return Collections.emptyList();
            }
            List<Table> tables = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object nameValue = getIgnoreCase(row, "OBJECT_NAME");
                if (nameValue == null) {
                    nameValue = getIgnoreCase(row, "TABLE_NAME");
                }
                if (nameValue == null || StringUtil.isBlank(String.valueOf(nameValue))) {
                    continue;
                }
                Table table = new Table();
                table.setName(String.valueOf(nameValue));
                table.setType(resolveTableType(getIgnoreCase(row, "OBJECT_TYPE")));
                tables.add(table);
            }
            return tables;
        });
    }

    private static String resolveTableType(Object typeValue) {
        if (typeValue == null) {
            return TableTypeEnum.TABLE.getCode();
        }
        String type = String.valueOf(typeValue).trim().toUpperCase(Locale.ROOT);
        if (type.contains("VIEW") && type.contains("MATERIALIZED")) {
            return TableTypeEnum.MATERIALIZED_VIEW.getCode();
        }
        if ("VIEW".equals(type)) {
            return TableTypeEnum.VIEW.getCode();
        }
        return TableTypeEnum.TABLE.getCode();
    }

    private static Object getIgnoreCase(Map<String, Object> row, String key) {
        if (row == null || StringUtil.isBlank(key)) {
            return null;
        }
        Object value = row.get(key);
        if (value != null) {
            return value;
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() != null && key.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * 从连接用户名解析 Schema：{@code SYS@oracle_t1} → {@code SYS}。
     */
    private static String resolveCurrentUser(DatabaseConnectorInstance connectorInstance) {
        if (connectorInstance == null || connectorInstance.getConfig() == null) {
            return null;
        }
        String username = connectorInstance.getConfig().getUsername();
        if (StringUtil.isBlank(username)) {
            return null;
        }
        String trimmed = username.trim();
        int at = trimmed.indexOf('@');
        String user = at > 0 ? trimmed.substring(0, at) : trimmed;
        return StringUtil.isBlank(user) ? null : user.toUpperCase(Locale.ROOT);
    }

    private static void ensureCurrentUserSchema(List<String> schemas, String currentUser) {
        if (StringUtil.isBlank(currentUser) || schemas == null) {
            return;
        }
        boolean exists = schemas.stream().anyMatch(s -> currentUser.equalsIgnoreCase(s));
        if (!exists) {
            schemas.add(0, currentUser);
        }
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:oceanbase://127.0.0.1:2881  （租户写在用户名 SYS@tenant）
        StringBuilder url = new StringBuilder();
        url.append("jdbc:oceanbase://").append(config.getHost()).append(":").append(config.getPort());
        if (StringUtil.isNotBlank(database)) {
            url.append("/").append(database.trim());
        }
        return url.toString();
    }

    /**
     * OceanBase Oracle 模式建用户：不支持 TEMPORARY TABLESPACE / QUOTA，仅 IDENTIFIED BY[+DEFAULT TABLESPACE]。
     */
    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        String user = schemaName.trim().toUpperCase(Locale.ROOT);
        String password = user + "@1234";
        String createUserSql = "CREATE USER " + user + " IDENTIFIED BY \"" + password + "\"";
        String grantSql = "GRANT CONNECT, RESOURCE TO " + user;
        return "DECLARE v_cnt NUMBER := 0; BEGIN " +
                "SELECT COUNT(1) INTO v_cnt FROM ALL_USERS WHERE USERNAME = '" + user + "'; " +
                "IF v_cnt = 0 THEN " +
                "EXECUTE IMMEDIATE '" + createUserSql + "'; " +
                "EXECUTE IMMEDIATE '" + grantSql + "'; " +
                "END IF; END;";
    }

    /**
     * 取源表 DDL：优先 DBMS_METADATA；失败时返回空串，由整库迁移按字段元数据拼建表语句。
     * <p>oceanbase-client / OB Oracle 对 GET_DDL、CLOB 支持不稳定，不可像 Oracle 一样直接抛错中断结构迁移。
     */
    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        if (sourceInstance == null || StringUtil.isBlank(sourceTableName)) {
            return StringUtil.EMPTY;
        }
        try {
            return sourceInstance.execute(databaseTemplate -> {
                String schema = resolveSchemaForDdl(sourceInstance);
                String table = sourceTableName.trim();
                String ddl = queryTableDdl(databaseTemplate, table, schema);
                if (StringUtil.isBlank(ddl)) {
                    String upperTable = table.toUpperCase(Locale.ROOT);
                    if (!upperTable.equals(table)) {
                        ddl = queryTableDdl(databaseTemplate, upperTable, schema);
                    }
                }
                return normalizeFetchedDdl(ddl, schema);
            });
        } catch (Exception e) {
            LOGGER.warn("OceanBaseOracle getSourceTableDDL failed for table={}, fallback to field DDL: {}",
                    sourceTableName, e.getMessage());
            return StringUtil.EMPTY;
        }
    }

    private static String resolveSchemaForDdl(DatabaseConnectorInstance sourceInstance) {
        String schema = sourceInstance.getSchema();
        if (StringUtil.isNotBlank(schema)) {
            return schema.trim().toUpperCase(Locale.ROOT);
        }
        return resolveCurrentUser(sourceInstance);
    }

    private String queryTableDdl(DatabaseTemplate databaseTemplate, String tableName, String schema) {
        try {
            if (StringUtil.isNotBlank(schema)) {
                return databaseTemplate.queryForObject(
                        "SELECT DBMS_METADATA.GET_DDL('TABLE', ?, ?) FROM DUAL",
                        (ResultSet rs, int rowNum) -> readDdlValue(rs),
                        tableName, schema);
            }
            return databaseTemplate.queryForObject(
                    "SELECT DBMS_METADATA.GET_DDL('TABLE', ?) FROM DUAL",
                    (ResultSet rs, int rowNum) -> readDdlValue(rs),
                    tableName);
        } catch (Exception e) {
            LOGGER.debug("DBMS_METADATA.GET_DDL failed table={}, schema={}: {}", tableName, schema, e.getMessage());
            return StringUtil.EMPTY;
        }
    }

    private static String readDdlValue(ResultSet rs) throws java.sql.SQLException {
        Object value = rs.getObject(1);
        if (value == null) {
            return StringUtil.EMPTY;
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            if (clob.length() == 0) {
                return StringUtil.EMPTY;
            }
            return clob.getSubString(1, (int) clob.length());
        }
        return String.valueOf(value);
    }

    private static String normalizeFetchedDdl(String ddl, String schema) {
        if (StringUtil.isBlank(ddl)) {
            return StringUtil.EMPTY;
        }
        ddl = ddl.trim();
        if (StringUtil.isNotBlank(schema)) {
            String quotedSchema = "\"" + schema.toUpperCase(Locale.ROOT) + "\"";
            ddl = ddl.replace(quotedSchema + ".", "");
            quotedSchema = "\"" + schema + "\"";
            ddl = ddl.replace(quotedSchema + ".", "");
        }
        if (ddl.endsWith(";")) {
            ddl = ddl.substring(0, ddl.length() - 1).trim();
        }
        return ddl;
    }

    /**
     * OceanBase 对 PARALLEL hint 支持有限，使用普通 COUNT 即可。
     */
    @Override
    public String getQueryCountSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String queryFilter = config.getQueryFilter();
        String query = "SELECT COUNT(*) FROM %s%s t %s";
        return String.format(query, config.getSchema(), database.buildWithQuotation(config.getTableName()), queryFilter);
    }

    /**
     * oceanbase-client 未实现 {@link java.sql.Connection#getSchema()}，不可回落到驱动方法。
     */
    @Override
    protected String getSchema(String schema, java.sql.Connection connection) {
        if (StringUtil.isNotBlank(schema)) {
            return schema.toUpperCase(Locale.ROOT);
        }
        return schema;
    }

    @Override
    public boolean isAlive(DatabaseConnectorInstance connectorInstance) {
        boolean alive = super.isAlive(connectorInstance);
        if (alive) {
            validateCompatibilityMode(connectorInstance);
        }
        return alive;
    }

    /**
     * 校验当前租户为 Oracle 兼容模式，避免误连 MySQL 租户导致后续元数据 SQL 失败。
     */
    private void validateCompatibilityMode(DatabaseConnectorInstance connectorInstance) {
        String mode = resolveCompatibilityMode(connectorInstance);
        if (StringUtil.isBlank(mode)) {
            LOGGER.warn("Unable to detect OceanBase ob_compatibility_mode, skip mode check");
            return;
        }
        if (!EXPECTED_MODE.equalsIgnoreCase(mode.trim())) {
            throw new SdkException("当前租户兼容模式为 " + mode + "，OceanBaseOracle 连接器仅支持 Oracle 模式。"
                    + "请改用 OceanBase 连接器，或连接 ob_compatibility_mode=oracle 的租户。");
        }
    }

    private String resolveCompatibilityMode(DatabaseConnectorInstance connectorInstance) {
        try {
            return connectorInstance.execute(databaseTemplate ->
                    databaseTemplate.queryForObject(QUERY_COMPAT_MODE_V, String.class));
        } catch (Exception e) {
            LOGGER.debug("Query V$OB_PARAMETERS failed: {}", e.getMessage());
        }
        try {
            return connectorInstance.execute(databaseTemplate -> {
                List<Map<String, Object>> rows = databaseTemplate.queryForList(QUERY_COMPAT_MODE_SHOW);
                if (CollectionUtils.isEmpty(rows)) {
                    return null;
                }
                Map<String, Object> row = rows.get(0);
                Object value = row.get("Value");
                if (value == null) {
                    value = row.get("VALUE");
                }
                if (value == null) {
                    value = row.get("value");
                }
                return value == null ? null : String.valueOf(value);
            });
        } catch (Exception e) {
            LOGGER.debug("SHOW VARIABLES ob_compatibility_mode failed: {}", e.getMessage());
            return null;
        }
    }
}
