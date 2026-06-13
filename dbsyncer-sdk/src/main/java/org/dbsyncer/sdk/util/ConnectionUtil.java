/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

/**
 * JDBC 连接工具
 */
public final class ConnectionUtil {

    private ConnectionUtil() {
    }

    /**
     * 判断是否为连接断开类异常（不应回收到连接池）
     */
    public static boolean isBrokenConnection(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase();
                if (lower.contains("broken pipe")
                        || lower.contains("communications link failure")
                        || lower.contains("connection reset")
                        || lower.contains("connection closed")
                        || lower.contains("nontransientconnection")
                        || lower.contains("could not create connection")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }
}
