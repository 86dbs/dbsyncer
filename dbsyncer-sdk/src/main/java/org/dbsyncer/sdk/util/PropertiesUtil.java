/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import java.util.Map;
import java.util.Properties;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-12-28 23:21
 */
public abstract class PropertiesUtil {

    /**
     * 将字符串解析为Properties对象
     *
     * @param params 格式：key1=value1&key2=value2
     * @return Properties对象
     */
    public static Properties parse(String params) {
        Properties properties = new Properties();
        if (params == null || params.trim().isEmpty()) {
            return properties;
        }
        // 按&分割参数
        String[] paramArray = params.split("&");
        for (String param : paramArray) {
            // 按=分割键值对
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                if (!key.isEmpty()) {
                    properties.put(key, value);
                }
            }
        }
        return properties;
    }

    /**
     * 将Properties对象转换为字符串
     *
     * @param properties Properties对象
     * @return 格式：key1=value1&key2=value2，如果properties为null或空，返回空字符串
     */
    public static String toString(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (!first) {
                sb.append("&");
            }
            String key = entry.getKey() != null ? entry.getKey().toString() : "";
            String value = entry.getValue() != null ? entry.getValue().toString() : "";
            sb.append(key).append("=").append(value);
            first = false;
        }
        return sb.toString();
    }
}
