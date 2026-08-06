/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.constant.PostgreSQLConfigConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022-04-05 22:14
 */
public class PostgreSQLConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);

        // 表单 checkbox：有键表示提交；复制展平可能带 "true"/"false"。缺键则保留 super 已还原的 extInfo
        if (params.containsKey(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE)) {
            String raw = params.get(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE);
            boolean on = StringUtil.isNotBlank(raw)
                    && !"false".equalsIgnoreCase(raw)
                    && !"0".equals(raw);
            connectorConfig.getExtInfo().put(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE, on ? "true" : "false");
        } else if (!connectorConfig.getExtInfo().containsKey(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE)) {
            connectorConfig.getExtInfo().put(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE, "true");
        }
        // params 缺省时（如复制）保留已从 extInfo JSON 还原的值，避免 Hashtable 拒 null
        String pluginName = params.get(PostgreSQLConfigConstant.PLUGIN_NAME);
        if (StringUtil.isBlank(pluginName)) {
            pluginName = connectorConfig.getExtInfo().getProperty(PostgreSQLConfigConstant.PLUGIN_NAME);
        }
        connectorConfig.getExtInfo().put(PostgreSQLConfigConstant.PLUGIN_NAME,
                StringUtil.getIfBlank(pluginName, "pgoutput"));
    }
}
