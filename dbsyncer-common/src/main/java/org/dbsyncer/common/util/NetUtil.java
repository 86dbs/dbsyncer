/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * 本机通告地址解析。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public abstract class NetUtil {

    private static final Logger logger = LoggerFactory.getLogger(NetUtil.class);

    private NetUtil() {
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
