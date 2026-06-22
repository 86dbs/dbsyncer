/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;

/**
 * OceanBase Binlog 服务连接配置（MySQL Binlog V4 协议）
 */
public final class OceanBaseBinlogConfig {

    public static final String BINLOG_HOST = "binlogHost";
    public static final String BINLOG_PORT = "binlogPort";

    private OceanBaseBinlogConfig() {
    }

    public static String getBinlogHost(DatabaseConfig config) {
        String binlogHost = config.getExtInfo().getProperty(BINLOG_HOST);
        return StringUtil.isBlank(binlogHost) ? config.getHost() : binlogHost.trim();
    }

    public static int getBinlogPort(DatabaseConfig config) {
        String binlogPort = config.getExtInfo().getProperty(BINLOG_PORT);
        if (StringUtil.isBlank(binlogPort)) {
            return config.getPort();
        }
        return NumberUtil.toInt(binlogPort.trim(), config.getPort());
    }
}
