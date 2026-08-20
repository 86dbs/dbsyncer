/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.sso;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.model.WebSsoTicket;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.web.sso.WebSsoTicketService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * 跨节点 Web SSO：已登录节点签发票据，目标节点消费后建立本地 Session。
 *
 * @author wuji
 * @version 1.0.0
 */
@Controller
@RequestMapping("/sso")
public class SsoController {

    private static final Logger logger = LoggerFactory.getLogger(SsoController.class);

    private static final String LOGIN_PAGE = "/login.html";

    @Resource
    private WebSsoTicketService webSsoTicketService;

    /**
     * 已登录用户跳转到目标节点（携带短时票据）。
     *
     * @param target   目标节点，格式 {@code ip:port}（勿带 http://，否则会被防火墙拦截）
     * @param redirect 目标相对路径，默认 /
     * @param response 响应
     */
    @GetMapping("/redirect")
    public void redirect(@RequestParam("target") String target,
                         @RequestParam(value = "redirect", required = false, defaultValue = "/") String redirect,
                         HttpServletResponse response) throws IOException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()
                || StringUtil.equals("anonymousUser", String.valueOf(authentication.getPrincipal()))) {
            response.sendRedirect(LOGIN_PAGE);
            return;
        }
        try {
            if (!webSsoTicketService.isAllowedTarget(target)) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "目标节点不在集群白名单中");
                return;
            }
            String username = authentication.getName();
            String roleCode = joinAuthorities(authentication.getAuthorities());
            String ticket = webSsoTicketService.issue(username, roleCode, target);
            String safeRedirect = webSsoTicketService.sanitizeRedirect(redirect);
            String base = webSsoTicketService.normalizeTargetBase(target);
            String consumeUrl = base + "/sso/consume?ticket="
                    + URLEncoder.encode(ticket, StandardCharsets.UTF_8.name())
                    + "&redirect=" + URLEncoder.encode(safeRedirect, StandardCharsets.UTF_8.name());
            response.sendRedirect(consumeUrl);
        } catch (BizException e) {
            logger.warn("SSO redirect 失败: {}", e.getMessage());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        } catch (Exception e) {
            logger.error("SSO redirect 异常", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "SSO 跳转失败");
        }
    }

    /**
     * 目标节点消费票据并建立本地登录态。
     *
     * @param ticket   JWT 票据
     * @param redirect 相对路径
     * @param request  请求
     * @param response 响应
     */
    @GetMapping("/consume")
    public void consume(@RequestParam("ticket") String ticket,
                        @RequestParam(value = "redirect", required = false, defaultValue = "/") String redirect,
                        HttpServletRequest request,
                        HttpServletResponse response) throws IOException {
        WebSsoTicket ssoTicket = webSsoTicketService.consume(ticket);
        if (ssoTicket == null) {
            response.sendRedirect(LOGIN_PAGE);
            return;
        }
        establishSession(request, ssoTicket);
        response.sendRedirect(webSsoTicketService.sanitizeRedirect(redirect));
    }

    private void establishSession(HttpServletRequest request, WebSsoTicket ssoTicket) {
        Collection<? extends GrantedAuthority> authorities =
                AuthorityUtils.commaSeparatedStringToAuthorityList(
                        StringUtil.getIfBlank(ssoTicket.getRoleCode(), StringUtil.EMPTY));
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken(ssoTicket.getUsername(), null, authorities);
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(auth);
        SecurityContextHolder.setContext(context);
        HttpSession session = request.getSession(true);
        session.setAttribute(HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY, context);
        logger.info("SSO 登录成功: user={}, session={}", ssoTicket.getUsername(), session.getId());
    }

    private String joinAuthorities(Collection<? extends GrantedAuthority> authorities) {
        if (CollectionUtils.isEmpty(authorities)) {
            return StringUtil.EMPTY;
        }
        return authorities.stream().map(GrantedAuthority::getAuthority).collect(Collectors.joining(","));
    }
}
