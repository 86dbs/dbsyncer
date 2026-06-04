/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

/**
 * OceanBase LogProxy CDC 配置常量
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-05 00:20
 */
public final class OceanBaseCdcConstants {

    public static final String LOG_PROXY_HOST = "logProxyHost";
    public static final String LOG_PROXY_PORT = "logProxyPort";
    public static final String TENANT_NAME = "tenantName";
    public static final String RS_LIST = "rsList";
    public static final String CONFIG_URL = "configUrl";
    public static final String WORKING_MODE = "workingMode";
    public static final String TIMEZONE = "timezone";
    public static final String SYS_USERNAME = "sysUsername";
    public static final String SYS_PASSWORD = "sysPassword";

    /** 增量位点：LogProxy safeTimestamp（秒） */
    public static final String SNAPSHOT_TIMESTAMP = "timestamp";

    public static final String OFFSET_LABEL = "logproxy";

    private OceanBaseCdcConstants() {
    }
}
