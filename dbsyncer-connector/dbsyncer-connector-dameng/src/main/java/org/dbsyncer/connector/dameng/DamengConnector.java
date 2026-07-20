/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.dameng.cdc.DamengListener;
import org.dbsyncer.connector.dameng.constant.DamengConstant;
import org.dbsyncer.connector.dameng.schema.DamengSchemaResolver;
import org.dbsyncer.connector.dameng.validator.DamengConfigValidator;
import org.dbsyncer.connector.oracle.OracleConnector;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.BindParameter;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.util.List;
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

    /**
     * 使用 ALL_USERS（非 DBA_USERS），普通用户也有权限查看 Schema 列表。
     */
    private static final String QUERY_SCHEMA = "SELECT USERNAME FROM ALL_USERS WHERE USERNAME NOT IN "
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
