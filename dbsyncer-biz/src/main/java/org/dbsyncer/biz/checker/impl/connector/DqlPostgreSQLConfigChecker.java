package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/5 22:14
 */
@Component
public class DqlPostgreSQLConfigChecker extends AbstractDataBaseConfigChecker {

    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifyDql(connectorConfig, params);
        super.modifySchema(connectorConfig, params);

        connectorConfig.getProperties().put("dropSlotOnClose", StringUtil.isNotBlank(params.get("dropSlotOnClose")) ? "true" : "false");
        connectorConfig.getProperties().put("pluginName", params.get("pluginName"));
    }
}