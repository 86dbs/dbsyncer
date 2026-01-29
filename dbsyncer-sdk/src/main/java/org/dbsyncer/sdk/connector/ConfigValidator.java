/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;

import java.util.Map;

/**
 * 连接器配置校验器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-07 23:17
 */
public interface ConfigValidator<S extends ConnectorService, C extends ConnectorConfig> {

    /**
     * 修改配置
     *
     * @param connectorService
     * @param connectorConfig
     * @param params
     * @return
     */
    void modify(S connectorService, C connectorConfig, Map<String, String> params);

    /**
     * 修改扩展表配置
     *
     * @param connectorService
     * @param params
     * @return
     */
    Table modifyExtendedTable(S connectorService, Map<String, String> params);
}