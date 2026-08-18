/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.cluster;

import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;

/**
 * 节点 HTTP / Raft 端口探测。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Component
@ConditionalOnProperty(prefix = "dbsyncer.cluster", name = "enabled", havingValue = "true")
public class NetworkProbe {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * HTTP ping 与 Raft TCP 均成功视为可达。
     *
     * @param ip       IP
     * @param httpPort HTTP 端口
     * @param raftPort Raft 端口
     * @return true 可达
     */
    public boolean isReachable(String ip, int httpPort, int raftPort) {
        return pingHttp(ip, httpPort) && pingTcp(ip, raftPort);
    }

    /**
     * 探测 HTTP /cluster/ping。
     *
     * @param ip   IP
     * @param port 端口
     * @return true 成功
     */
    public boolean pingHttp(String ip, int port) {
        if (StringUtil.isBlank(ip) || port <= 0) {
            return false;
        }
        HttpURLConnection conn = null;
        try {
            URL url = new URL("http://" + ip + ":" + port + "/cluster/ping");
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            conn.setRequestMethod("GET");
            int code = conn.getResponseCode();
            return code >= 200 && code < 400;
        } catch (Exception e) {
            logger.debug("HTTP ping 失败 {}:{} {}", ip, port, e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * TCP 探测端口。
     *
     * @param ip   IP
     * @param port 端口
     * @return true 可连接
     */
    public boolean pingTcp(String ip, int port) {
        if (StringUtil.isBlank(ip) || port <= 0) {
            return false;
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), 2000);
            return true;
        } catch (IOException e) {
            logger.debug("TCP ping 失败 {}:{} {}", ip, port, e.getMessage());
            return false;
        }
    }
}
