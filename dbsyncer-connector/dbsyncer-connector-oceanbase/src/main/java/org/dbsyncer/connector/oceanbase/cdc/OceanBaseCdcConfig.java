/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;

import java.util.Properties;

/**
 * OceanBase LogProxy CDC 连接配置
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-05 00:20
 */
public final class OceanBaseCdcConfig {

    private static final int DEFAULT_LOG_PROXY_PORT = 2983;
    private static final String DEFAULT_WORKING_MODE = "storage";
    private static final String DEFAULT_TIMEZONE = "+08:00";

    private final String logProxyHost;
    private final int logProxyPort;
    private final String tenantName;
    private final String rsList;
    private final String configUrl;
    private final String workingMode;
    private final String timezone;
    private final String sysUsername;
    private final String sysPassword;
    private final String username;
    private final String password;

    public OceanBaseCdcConfig(DatabaseConfig config) {
        Properties ext = config.getExtInfo() != null ? config.getExtInfo() : new Properties();
        this.logProxyHost = ext.getProperty(OceanBaseCdcConstants.LOG_PROXY_HOST);
        this.logProxyPort = NumberUtil.toInt(ext.getProperty(OceanBaseCdcConstants.LOG_PROXY_PORT), DEFAULT_LOG_PROXY_PORT);
        this.tenantName = ext.getProperty(OceanBaseCdcConstants.TENANT_NAME);
        this.rsList = ext.getProperty(OceanBaseCdcConstants.RS_LIST);
        this.configUrl = ext.getProperty(OceanBaseCdcConstants.CONFIG_URL);
        this.workingMode = StringUtil.isBlank(ext.getProperty(OceanBaseCdcConstants.WORKING_MODE))
                ? DEFAULT_WORKING_MODE
                : ext.getProperty(OceanBaseCdcConstants.WORKING_MODE);
        this.timezone = StringUtil.isBlank(ext.getProperty(OceanBaseCdcConstants.TIMEZONE))
                ? DEFAULT_TIMEZONE
                : ext.getProperty(OceanBaseCdcConstants.TIMEZONE);
        this.sysUsername = ext.getProperty(OceanBaseCdcConstants.SYS_USERNAME);
        this.sysPassword = ext.getProperty(OceanBaseCdcConstants.SYS_PASSWORD);
        this.username = config.getUsername();
        this.password = config.getPassword();
    }

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public int getLogProxyPort() {
        return logProxyPort;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getRsList() {
        return rsList;
    }

    public String getConfigUrl() {
        return configUrl;
    }

    public String getWorkingMode() {
        return workingMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getSysUsername() {
        return sysUsername;
    }

    public String getSysPassword() {
        return sysPassword;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * 解析 LogProxy 用户名，格式 user@tenant
     */
    public String resolveUsername() {
        if (StringUtil.isBlank(username)) {
            return username;
        }
        if (username.contains("@") || StringUtil.isBlank(tenantName)) {
            return username;
        }
        return username + "@" + tenantName;
    }
}
