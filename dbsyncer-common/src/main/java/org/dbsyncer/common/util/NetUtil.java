/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.HttpURLConnection;
import java.security.cert.X509Certificate;

/**
 * 本机通告地址解析与节点 Web 根地址拼装。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public abstract class NetUtil {

    private static final HostnameVerifier TRUST_ALL_HOSTNAME = (hostname, session) -> true;

    private static volatile SSLSocketFactory trustAllSocketFactory;

    private NetUtil() {
    }

    /**
     * 拼装节点 Web 根地址（无尾斜杠）。
     *
     * @param ip   IP
     * @param port 端口
     * @param ssl  是否 HTTPS（对应 {@code server.ssl.enabled}）
     * @return 如 {@code http://ip:port} / {@code https://ip:port}；非法时为空
     */
    public static String buildWebRootUrl(String ip, int port, boolean ssl) {
        if (StringUtil.isBlank(ip) || port <= 0) {
            return StringUtil.EMPTY;
        }
        return (ssl ? "https://" : "http://") + ip + ":" + port;
    }

    /**
     * 节点间互探若走 HTTPS（自签证书），跳过证书与主机名校验。
     *
     * @param connection 已 open 的连接
     */
    public static void applyInsecureSslIfNeeded(HttpURLConnection connection) {
        if (!(connection instanceof HttpsURLConnection)) {
            return;
        }
        HttpsURLConnection https = (HttpsURLConnection) connection;
        https.setSSLSocketFactory(getTrustAllSocketFactory());
        https.setHostnameVerifier(TRUST_ALL_HOSTNAME);
    }

    private static SSLSocketFactory getTrustAllSocketFactory() {
        SSLSocketFactory factory = trustAllSocketFactory;
        if (factory != null) {
            return factory;
        }
        synchronized (NetUtil.class) {
            if (trustAllSocketFactory != null) {
                return trustAllSocketFactory;
            }
            try {
                TrustManager[] trustAll = new TrustManager[]{new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }};
                SSLContext context = SSLContext.getInstance("TLS");
                context.init(null, trustAll, null);
                trustAllSocketFactory = context.getSocketFactory();
                return trustAllSocketFactory;
            } catch (Exception e) {
                throw new IllegalStateException("初始化 HTTPS TrustAll 失败", e);
            }
        }
    }
}
