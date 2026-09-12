/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.HttpClientUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.UrlPathHelper;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * 校验节点间共享密钥，保护 {@code /cluster/internal/**} 与 {@code /cluster/metrics}。
 * <p>
 * 配置项 {@code dbsyncer.cluster.internal-token} 未设置时一律拒绝（生产默认安全）。
 * 请求须携带头 {@link HttpClientUtil#CLUSTER_TOKEN_HEADER}。
 * {@code /cluster/ping} 仅存活探测，不在本过滤器范围内。
 *
 * @author wuji
 * @version 1.0.0
 */
public class ClusterInternalAuthFilter extends OncePerRequestFilter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String PRINCIPAL = "cluster-internal";

    private static final String ROLE = "ROLE_CLUSTER_INTERNAL";

    private final UrlPathHelper urlPathHelper = new UrlPathHelper();

    @Value("${dbsyncer.cluster.internal-token:}")
    private String internalToken;

    @Value("${dbsyncer.cluster.enabled:false}")
    private boolean clusterEnabled;

    @PostConstruct
    public void logConfig() {
        if (StringUtil.isBlank(internalToken)) {
            logger.warn("dbsyncer.cluster.internal-token 未配置，/cluster/internal/** 与 /cluster/metrics 将被拒绝。生产集群必须为各节点配置相同共享密钥，并通过请求头 {} 传递。", HttpClientUtil.CLUSTER_TOKEN_HEADER);
            return;
        }
        if (clusterEnabled) {
            logger.info("集群内部控制面已启用 {} 校验", HttpClientUtil.CLUSTER_TOKEN_HEADER);
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // 获取请求路径，避免用户请求/.;之类的字符绕过权限判断，导致绕过权限检查风险。
        String path = urlPathHelper.getLookupPathForRequest(request);
        if (StringUtil.isNotBlank(path)) {
            return !path.startsWith("/cluster/internal/") && !"/cluster/metrics".equals(path);
        }
        return false;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        if (StringUtil.isBlank(internalToken)) {
            reject(response, HttpServletResponse.SC_FORBIDDEN, "集群内部接口未配置 dbsyncer.cluster.internal-token，已拒绝访问");
            return;
        }
        String provided = request.getHeader(HttpClientUtil.CLUSTER_TOKEN_HEADER);
        if (!matches(internalToken, provided)) {
            reject(response, HttpServletResponse.SC_UNAUTHORIZED, "集群内部接口鉴权失败");
            return;
        }
        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(PRINCIPAL, "N/A", AuthorityUtils.createAuthorityList(ROLE)));
        filterChain.doFilter(request, response);
    }

    /**
     * 常量时间比较，避免短 token 被逐字节探测。
     *
     * @param expected 配置的共享密钥
     * @param provided 请求头
     * @return 是否一致
     */
    private boolean matches(String expected, String provided) {
        if (expected == null || provided == null) {
            return false;
        }
        byte[] left = expected.getBytes(StandardCharsets.UTF_8);
        byte[] right = provided.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(left, right);
    }

    private void reject(HttpServletResponse response, int status, String message) throws IOException {
        response.setContentType("application/json;charset=utf-8");
        response.setStatus(status);
        try (PrintWriter out = response.getWriter()) {
            out.write(JsonUtil.objToJson(RestResult.restFail(message, status)));
            out.flush();
        }
    }
}
