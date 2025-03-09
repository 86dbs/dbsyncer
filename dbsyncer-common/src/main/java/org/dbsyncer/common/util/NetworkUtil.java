/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-03-06 00:42
 */
public abstract class NetworkUtil {

    public static boolean isInternetAvailable() {
        HttpURLConnection connection = null;
        try {
            URL url = new URL("https://www.baidu.com");
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(5000);
            connection.connect();
            return connection.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

}