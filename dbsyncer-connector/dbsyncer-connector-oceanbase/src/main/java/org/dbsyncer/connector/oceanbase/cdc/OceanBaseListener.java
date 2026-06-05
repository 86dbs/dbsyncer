/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

import org.dbsyncer.connector.mysql.binlog.BinaryLogClient;
import org.dbsyncer.connector.mysql.binlog.BinaryLogRemoteClient;
import org.dbsyncer.connector.mysql.cdc.MySQLListener;
import org.dbsyncer.connector.oceanbase.OceanBaseException;
import org.dbsyncer.sdk.config.DatabaseConfig;

/**
 * OceanBase 增量监听，基于 Binlog 服务输出的 MySQL Binlog V4 协议
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-05 00:20
 */
public final class OceanBaseListener extends MySQLListener {

    @Override
    protected BinaryLogClient createBinlogClient(DatabaseConfig config) throws Exception {
        String host = OceanBaseBinlogConfig.getBinlogHost(config);
        int port = OceanBaseBinlogConfig.getBinlogPort(config);
        logger.info("连接 OceanBase Binlog 服务 {}:{}", host, port);
        return new BinaryLogRemoteClient(host, port, config.getUsername(), config.getPassword());
    }

    @Override
    protected String getAlreadyStartedMessage() {
        return "OceanBase Binlog 监听器已启动";
    }

    @Override
    protected String getCaptureSnapshotFailedMessage() {
        return "捕获 OceanBase Binlog 位点失败:{}";
    }

    @Override
    protected RuntimeException wrapException(Exception e) {
        return new OceanBaseException(e);
    }

    @Override
    protected RuntimeException wrapException(String message) {
        return new OceanBaseException(message);
    }
}
