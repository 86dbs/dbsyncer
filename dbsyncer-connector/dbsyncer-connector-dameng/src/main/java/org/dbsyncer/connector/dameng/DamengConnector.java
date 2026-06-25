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
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.List;

/**
 * 达梦（DM）连接器（兼容 Oracle 语法）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 02:00
 */
public final class DamengConnector extends OracleConnector {

    private static final String QUERY_SCHEMA = "SELECT USERNAME FROM DBA_USERS WHERE USERNAME NOT IN "
            + "('SYS','SYSSSO','SYSAUDITOR','SYSDBO','CTISYS') ORDER BY USERNAME";

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

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:dm://").append(config.getHost()).append(":").append(config.getPort());
        if (StringUtil.isNotBlank(database)) {
            url.append("/").append(database);
        }
        return url.toString();
    }
}
