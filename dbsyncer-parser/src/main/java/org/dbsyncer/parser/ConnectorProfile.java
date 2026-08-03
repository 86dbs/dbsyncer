/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.Connector;

import java.util.List;

/**
 * 连接器配置读写与反序列化（抽象 {@code ConnectorConfig} 按 connectorType 还原）。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface ConnectorProfile {

    /**
     * 连接器 JSON → 模型（含具体 Config 实现类）。
     */
    Connector parseConnector(String json);

    /**
     * 按 id 查询连接器。
     */
    Connector getConnector(String connectorId);

    /**
     * 全部连接器。
     */
    List<Connector> getConnectorAll();
}
