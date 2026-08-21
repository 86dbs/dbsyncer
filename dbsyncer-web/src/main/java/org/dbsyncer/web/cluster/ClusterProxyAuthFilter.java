/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.cluster;

import org.dbsyncer.biz.model.WebSsoTicket;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;

/**
 * Leader 侧：校验写代理头并建立请求级认证（不写入 Session，避免挤掉本机登录）。
 *
 * @author wuji
 * @version 1.0.0
 */
public class ClusterProxyAuthFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(ClusterProxyAuthFilter.class);

    private final ClusterWriteProxyService clusterWriteProxyService;

    public ClusterProxyAuthFilter(ClusterWriteProxyService clusterWriteProxyService) {
        this.clusterWriteProxyService = clusterWriteProxyService;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String raw = request.getHeader(ClusterWriteProxyPaths.PROXY_HEADER);
        if (StringUtil.isBlank(raw)) {
            filterChain.doFilter(request, response);
            return;
        }
        WebSsoTicket ticket = clusterWriteProxyService.verify(raw);
        if (ticket == null) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "集群写代理票据无效");
            return;
        }
        Collection<? extends GrantedAuthority> authorities =
                AuthorityUtils.commaSeparatedStringToAuthorityList(
                        StringUtil.getIfBlank(ticket.getRoleCode(), StringUtil.EMPTY));
        if (CollectionUtils.isEmpty(authorities)) {
            authorities = AuthorityUtils.NO_AUTHORITIES;
        }
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken(ticket.getUsername(), null, authorities);
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(auth);
        SecurityContextHolder.setContext(context);
        try {
            filterChain.doFilter(request, response);
        } finally {
            SecurityContextHolder.clearContext();
            logger.debug("写代理请求结束, user={}", ticket.getUsername());
        }
    }
}
