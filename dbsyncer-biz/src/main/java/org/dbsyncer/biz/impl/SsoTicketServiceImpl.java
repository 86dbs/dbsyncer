/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.SsoTicketService;
import org.dbsyncer.biz.model.WebSsoTicket;
import org.dbsyncer.common.util.NetUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.sdk.spi.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Web 控制台跨节点 SSO：短时票据签发、白名单校验、一次性消费。
 *
 * @author wuji
 * @version 1.0.0
 */
@Service
public class SsoTicketServiceImpl implements SsoTicketService {

    private static final Logger logger = LoggerFactory.getLogger(SsoTicketServiceImpl.class);

    /**
     * 票据有效期（毫秒）
     */
    private static final long TICKET_TTL_MS = 30_000L;

    @Value("${server.ssl.enabled:false}")
    private boolean sslEnabled;

    @Resource
    private JwtSecretManager jwtSecretManager;

    @Resource
    private ClusterService clusterService;

    /**
     * 已消费 jti → 过期时间，本机防重放
     */
    private final Map<String, Long> usedJti = new ConcurrentHashMap<>();

    /**
     * 签发跳转票据。
     *
     * @param username 用户名
     * @param roleCode 角色
     * @param target   目标节点，推荐 {@code ip:port}；也兼容 {@code http(s)://ip:port}
     * @return JWT 票据
     */
    @Override
    public String issue(String username, String roleCode, String target) {
        if (StringUtil.isBlank(username) || StringUtil.isBlank(target)) {
            throw new BizException("SSO 参数不完整");
        }
        String targetHost = resolveTargetHost(target);
        long now = System.currentTimeMillis();
        WebSsoTicket ticket = new WebSsoTicket();
        ticket.setUsername(username);
        ticket.setRoleCode(StringUtil.getIfBlank(roleCode, StringUtil.EMPTY));
        ticket.setJti(UUIDUtil.getUUID());
        ticket.setTargetHost(targetHost);
        ticket.setIat(now);
        ticket.setExp(now + TICKET_TTL_MS);
        try {
            return jwtSecretManager.signPayload(ticket);
        } catch (Exception e) {
            logger.error("签发 SSO 票据失败", e);
            throw new BizException("签发 SSO 票据失败");
        }
    }

    /**
     * 校验并消费票据（一次性）。
     *
     * @param rawTicket JWT
     * @return 有效票据；失败返回 null
     */
    @Override
    public WebSsoTicket consume(String rawTicket) {
        purgeExpiredJti();
        if (StringUtil.isBlank(rawTicket)) {
            return null;
        }
        WebSsoTicket ticket;
        try {
            ticket = jwtSecretManager.verifyPayload(rawTicket, WebSsoTicket.class);
        } catch (Exception e) {
            logger.warn("SSO 票据验签异常: {}", e.getMessage());
            return null;
        }
        if (ticket == null || StringUtil.isBlank(ticket.getUsername()) || StringUtil.isBlank(ticket.getJti())) {
            return null;
        }
        Long exp = ticket.getExp();
        if (exp == null || exp < System.currentTimeMillis()) {
            logger.warn("SSO 票据已过期, jti={}", ticket.getJti());
            return null;
        }
        String localHost = clusterService.getLocalNodeId();
        if (StringUtil.isBlank(ticket.getTargetHost()) || !StringUtil.equals(ticket.getTargetHost(), localHost)) {
            logger.warn("SSO 票据目标不匹配, expect={}, actual={}", localHost, ticket.getTargetHost());
            return null;
        }
        Long prev = usedJti.putIfAbsent(ticket.getJti(), exp);
        if (prev != null) {
            logger.warn("SSO 票据重放, jti={}", ticket.getJti());
            return null;
        }
        return ticket;
    }

    /**
     * 目标 URL 是否属于集群节点。
     *
     * @param target 目标 URL
     * @return true 允许
     */
    @Override
    public boolean isAllowedTarget(String target) {
        String nodeId = resolveTargetHost(target);
        return clusterService.getNode(nodeId) != null;
    }

    /**
     * 规范化为 {@code http(s)://ip:port}（无尾斜杠；协议跟随 {@code server.ssl.enabled}）。
     *
     * @param target 目标 URL
     * @return 根地址
     */
    @Override
    public String normalizeTargetBase(String target) {
        String host = resolveTargetHost(target);
        int idx = host.lastIndexOf(':');
        if (idx <= 0) {
            throw new BizException("无效的目标地址");
        }
        String ip = host.substring(0, idx);
        int port = Integer.parseInt(host.substring(idx + 1));
        return NetUtil.buildWebRootUrl(ip, port, sslEnabled);
    }

    /**
     * 仅允许相对路径，防止开放重定向。
     *
     * @param redirect 跳转路径
     * @return 安全路径
     */
    @Override
    public String sanitizeRedirect(String redirect) {
        if (StringUtil.isBlank(redirect)) {
            return "/";
        }
        String path = redirect.trim();
        if (!path.startsWith("/") || path.startsWith("//") || path.contains("://")) {
            return "/";
        }
        return path;
    }

    private String resolveTargetHost(String target) {
        if (StringUtil.isBlank(target)) {
            throw new BizException("目标地址为空");
        }
        String trimmed = target.trim();
        try {
            URL url = new URL(trimmed.contains("://") ? trimmed : "http://" + trimmed);
            String host = url.getHost();
            int port = url.getPort();
            if (port < 0) {
                port = url.getDefaultPort();
            }
            if (StringUtil.isBlank(host) || port <= 0) {
                throw new BizException("无效的目标地址");
            }
            return host + ":" + port;
        } catch (MalformedURLException e) {
            throw new BizException("无效的目标地址");
        }
    }

    private void purgeExpiredJti() {
        long now = System.currentTimeMillis();
        usedJti.entrySet().removeIf(e -> e.getValue() == null || e.getValue() < now);
    }
}
