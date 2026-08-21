/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.HttpURLConnection;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

/**
 * 本机通告地址解析与节点 Web 根地址拼装。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public abstract class NetUtil {

    private static final Logger logger = LoggerFactory.getLogger(NetUtil.class);

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

    /**
     * 解析对外 IP：已显式配置则使用（含本机联调的 127.0.0.1）；未配 / 0.0.0.0 / localhost 则探测非回环 IPv4。
     *
     * @param configured 配置的 server.ip，可空
     * @return 通告 IP
     */
    public static String resolveAdvertiseIp(String configured) {
        if (isUsableAdvertise(configured)) {
            return configured.trim();
        }
        String detected = detectNonLoopbackIpv4();
        if (StringUtil.isNotBlank(detected)) {
            logger.warn("server.ip 未配置，自动探测为 {}；容器/多网卡请显式配置 server.ip", detected);
            return detected;
        }
        logger.warn("未能探测到非回环 IP，回落 127.0.0.1");
        return "127.0.0.1";
    }

    /**
     * 配置值是否可直接作为通告地址。
     *
     * @param configured 配置
     * @return true 可用
     */
    public static boolean isUsableAdvertise(String configured) {
        if (StringUtil.isBlank(configured)) {
            return false;
        }
        String ip = configured.trim();
        return !StringUtil.equals("0.0.0.0", ip)
                && !StringUtil.equalsIgnoreCase("localhost", ip);
    }

    private static String detectNonLoopbackIpv4() {
        try {
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            if (nics == null) {
                return fallbackLocalHost();
            }
            while (nics.hasMoreElements()) {
                NetworkInterface nic = nics.nextElement();
                if (nic == null || nic.isLoopback() || !nic.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addrs = nic.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress addr = addrs.nextElement();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("探测网卡 IP 失败: {}", e.getMessage());
        }
        return fallbackLocalHost();
    }

    private static String fallbackLocalHost() {
        try {
            InetAddress local = InetAddress.getLocalHost();
            if (local != null && !local.isLoopbackAddress()) {
                return local.getHostAddress();
            }
        } catch (Exception e) {
            logger.debug("getLocalHost 失败: {}", e.getMessage());
        }
        return null;
    }
}
