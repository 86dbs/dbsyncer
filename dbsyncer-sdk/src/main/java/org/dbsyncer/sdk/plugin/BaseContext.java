/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 00:59
 */
public interface BaseContext {

    /**
     * 执行命令
     */
    Map<String, String> getCommand();

    void setCommand(Map<String, String> command);

    /**
     * 数据源连接实例
     */
    ConnectorInstance getSourceConnectorInstance();

    void setSourceConnectorInstance(ConnectorInstance sourceConnectorInstance);

}