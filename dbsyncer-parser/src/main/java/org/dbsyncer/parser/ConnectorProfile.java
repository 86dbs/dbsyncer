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

    /**
     * 添加连接器。
     */
    String addConnector(Connector connector);

    /**
     * 批量添加连接器（配置包导入）。
     */
    void addConnectorBatch(List<Connector> connectors);

    /**
     * 更新连接器。
     */
    String updateConnector(Connector connector);

    /**
     * 删除连接器。
     */
    void removeConnector(String id);

    /**
     * 连接器总数。
     */
    int countConnectors();

    /**
     * 从 connector.json 数组批量导入。
     */
    void importConnectorsFromJson(String json);
}
