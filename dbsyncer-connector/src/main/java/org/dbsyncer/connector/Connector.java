package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;

import java.util.List;

/**
 * 连接器基础功能
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/18 23:30
 */
public interface Connector {

    /**
     * 检查连接器是否连接正常
     *
     * @param config 连接器配置
     * @return
     */
    boolean isAlive(ConnectorConfig config);

    /**
     * 获取所有表名
     *
     * @param config
     * @return
     */
    List<String> getTable(ConnectorConfig config);

    /**
     * 获取表元信息
     *
     * @param config
     * @param tableName
     * @return
     */
    MetaInfo getMetaInfo(ConnectorConfig config, String tableName);
}