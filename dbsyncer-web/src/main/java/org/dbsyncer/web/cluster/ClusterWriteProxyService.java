/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.cluster;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.biz.model.WebSsoTicket;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.web.sso.WebSsoTicketService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * 集群写代理票据：Follower 签发，Leader 校验（短时、绑定目标节点）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Service
public class ClusterWriteProxyService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterWriteProxyService.class);

    private static final long TICKET_TTL_MS = 30_000L;

    @Resource
    private JwtSecretManager jwtSecretManager;

    @Resource
    private ClusterService clusterService;

    @Resource
    private WebSsoTicketService webSsoTicketService;

    /**
     * 为当前用户签发指向 Leader 的代理票据。
     *
     * @param username 用户名
     * @param roleCode 角色
     * @return JWT
     */
    public String issueForLeader(String username, String roleCode) {
        if (StringUtil.isBlank(username)) {
            throw new BizException("代理鉴权用户为空");
        }
        String leaderUrl = clusterService.getLeaderHttpUrl();
        if (StringUtil.isBlank(leaderUrl)) {
            throw new BizException("当前无可用 Leader");
        }
        String targetHost = resolveHost(leaderUrl);
        if (!webSsoTicketService.isAllowedTarget(targetHost)) {
            throw new BizException("Leader 不在集群白名单中");
        }
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
            logger.error("签发写代理票据失败", e);
            throw new BizException("签发写代理票据失败");
        }
    }

    /**
     * 校验代理票据（本机须为票据目标）。
     *
     * @param rawTicket JWT
     * @return 有效票据；失败返回 null
     */
    public WebSsoTicket verify(String rawTicket) {
        if (StringUtil.isBlank(rawTicket)) {
            return null;
        }
        try {
            WebSsoTicket ticket = jwtSecretManager.verifyPayload(rawTicket, WebSsoTicket.class);
            if (ticket == null || StringUtil.isBlank(ticket.getUsername()) || StringUtil.isBlank(ticket.getJti())) {
                return null;
            }
            long now = System.currentTimeMillis();
            if (ticket.getExp() == null || ticket.getExp() < now) {
                return null;
            }
            String local = clusterService.getLocalNodeId();
            if (StringUtil.isBlank(local) || !StringUtil.equals(local, ticket.getTargetHost())) {
                logger.warn("写代理票据目标不匹配, local={}, target={}", local, ticket.getTargetHost());
                return null;
            }
            return ticket;
        } catch (Exception e) {
            logger.warn("写代理票据校验失败: {}", e.getMessage());
            return null;
        }
    }

    private String resolveHost(String leaderUrl) {
        String trimmed = leaderUrl.trim();
        int scheme = trimmed.indexOf("://");
        String hostPort = scheme >= 0 ? trimmed.substring(scheme + 3) : trimmed;
        int slash = hostPort.indexOf('/');
        if (slash >= 0) {
            hostPort = hostPort.substring(0, slash);
        }
        return hostPort;
    }
}
