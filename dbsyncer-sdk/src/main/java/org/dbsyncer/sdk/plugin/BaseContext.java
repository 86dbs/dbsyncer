/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.rsa.RsaManager;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Table;

import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 00:59
 */
public interface BaseContext {

    /**
     * 获取源表信息
     */
    Table getSourceTable();

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

    /**
     * 获取RSA加密类（http连接器场景）
     */
    RsaManager getRsaManager();

    /**
     * 获取RSA配置（http连接器场景）
     */
    RsaConfig getRsaConfig();
}
