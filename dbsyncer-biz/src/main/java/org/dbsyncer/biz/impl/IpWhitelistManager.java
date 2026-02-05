/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.common.model.IpWhitelistConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * IP白名单管理器
 * 负责IP白名单的验证和管理
 * 支持单个IP、IP段（CIDR）和IP范围
 * 
 * @author 穿云
 * @version 1.0.0
 */
@Component
public class IpWhitelistManager {

    private static final Logger logger = LoggerFactory.getLogger(IpWhitelistManager.class);

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private ProfileComponent profileComponent;

    /**
     * 验证IP是否在白名单中
     * 
     * @param clientIp 客户端IP地址
     * @return 是否在白名单中
     */
    public boolean isAllowed(String clientIp) {
        try {
            if (StringUtil.isBlank(clientIp)) {
                logger.warn("客户端IP为空");
                return false;
            }

            IpWhitelistConfig config = getIpWhitelistConfig();
            if (config == null || !config.isEnabled()) {
                // 如果未启用白名单，默认允许所有IP
                return true;
            }

            // 允许localhost（如果配置允许）
            if (config.isAllowLocalhost() && isLocalhost(clientIp)) {
                logger.debug("允许localhost访问: {}", clientIp);
                return true;
            }

            // 检查白名单
            List<String> whitelist = config.getWhitelist();
            if (whitelist == null || whitelist.isEmpty()) {
                logger.warn("IP白名单为空，拒绝访问");
                return false;
            }

            // 遍历白名单，检查是否匹配
            for (String ipPattern : whitelist) {
                if (StringUtil.isBlank(ipPattern)) {
                    continue;
                }

                if (matchesIpPattern(clientIp, ipPattern.trim())) {
                    logger.debug("IP {} 匹配白名单规则: {}", clientIp, ipPattern);
                    return true;
                }
            }

            logger.warn("IP {} 不在白名单中", clientIp);
            return false;
        } catch (Exception e) {
            logger.error("验证IP白名单失败", e);
            // 验证失败时，为了安全起见，拒绝访问
            return false;
        }
    }

    /**
     * 检查IP是否匹配规则
     * 支持：
     * - 单个IP: 192.168.1.1
     * - CIDR: 192.168.1.0/24
     * - IP范围: 192.168.1.1-192.168.1.100
     * 
     * @param clientIp 客户端IP
     * @param pattern IP规则
     * @return 是否匹配
     */
    private boolean matchesIpPattern(String clientIp, String pattern) {
        try {
            // 精确匹配
            if (clientIp.equals(pattern)) {
                return true;
            }

            // CIDR格式: 192.168.1.0/24
            if (pattern.contains("/")) {
                return matchesCidr(clientIp, pattern);
            }

            // IP范围格式: 192.168.1.1-192.168.1.100
            if (pattern.contains("-")) {
                return matchesIpRange(clientIp, pattern);
            }

            // 简单匹配
            return clientIp.equals(pattern);
        } catch (Exception e) {
            logger.warn("匹配IP规则失败: {} - {}", clientIp, pattern, e);
            return false;
        }
    }

    /**
     * 检查IP是否在CIDR范围内
     * 
     * @param clientIp 客户端IP
     * @param cidr CIDR格式，如: 192.168.1.0/24
     * @return 是否在范围内
     */
    private boolean matchesCidr(String clientIp, String cidr) {
        try {
            String[] parts = cidr.split("/");
            if (parts.length != 2) {
                return false;
            }

            String networkIp = parts[0];
            int prefixLength = Integer.parseInt(parts[1]);

            if (prefixLength < 0 || prefixLength > 32) {
                return false;
            }

            long clientIpLong = ipToLong(clientIp);
            long networkIpLong = ipToLong(networkIp);
            long mask = (0xFFFFFFFFL << (32 - prefixLength)) & 0xFFFFFFFFL;

            return (clientIpLong & mask) == (networkIpLong & mask);
        } catch (Exception e) {
            logger.warn("CIDR匹配失败: {} - {}", clientIp, cidr, e);
            return false;
        }
    }

    /**
     * 检查IP是否在IP范围内
     * 
     * @param clientIp 客户端IP
     * @param ipRange IP范围，如: 192.168.1.1-192.168.1.100
     * @return 是否在范围内
     */
    private boolean matchesIpRange(String clientIp, String ipRange) {
        try {
            String[] parts = ipRange.split("-");
            if (parts.length != 2) {
                return false;
            }

            String startIp = parts[0].trim();
            String endIp = parts[1].trim();

            long clientIpLong = ipToLong(clientIp);
            long startIpLong = ipToLong(startIp);
            long endIpLong = ipToLong(endIp);

            return clientIpLong >= startIpLong && clientIpLong <= endIpLong;
        } catch (Exception e) {
            logger.warn("IP范围匹配失败: {} - {}", clientIp, ipRange, e);
            return false;
        }
    }

    /**
     * 将IP地址转换为长整型
     * 
     * @param ip IP地址
     * @return 长整型值
     */
    private long ipToLong(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid IP address: " + ip);
        }

        long result = 0;
        for (int i = 0; i < 4; i++) {
            int part = Integer.parseInt(parts[i]);
            if (part < 0 || part > 255) {
                throw new IllegalArgumentException("Invalid IP address: " + ip);
            }
            result = (result << 8) + part;
        }
        return result;
    }

    /**
     * 检查是否为localhost
     * 
     * @param ip IP地址
     * @return 是否为localhost
     */
    private boolean isLocalhost(String ip) {
        if (StringUtil.isBlank(ip)) {
            return false;
        }

        // 检查常见的localhost地址
        return "127.0.0.1".equals(ip) ||
               "0:0:0:0:0:0:0:1".equals(ip) ||
               "::1".equals(ip) ||
               "localhost".equalsIgnoreCase(ip);
    }

    /**
     * 从系统配置中获取IP白名单配置
     * 
     * @return IP白名单配置，如果不存在返回null
     */
    private IpWhitelistConfig getIpWhitelistConfig() {
        try {
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null) {
                return null;
            }

            return systemConfig.getIpWhitelistConfig();
        } catch (Exception e) {
            logger.error("获取IP白名单配置失败", e);
            return null;
        }
    }

}
