/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.util;

import org.dbsyncer.sdk.util.PropertiesUtil;

import java.util.Properties;

/**
 * Http 连接器工具类
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-12 01:04
 */
public abstract class HttpUtil {

    /**
     * 解析连接参数（支持换行符或 & 分隔）
     */
    public static Properties parse(String properties) {
        if (properties == null || properties.trim().isEmpty()) {
            return new Properties();
        }
        String normalized = properties.replaceAll("\r\n", "&").replaceAll("\n", "&");
        return PropertiesUtil.parse(normalized);
    }

    /**
     * 将 Properties 转为字符串（换行分隔，便于页面展示）
     */
    public static String toString(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        String text = PropertiesUtil.toString(properties);
        return text.replaceAll("&", "\n");
    }
}
