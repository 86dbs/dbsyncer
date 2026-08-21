/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.cluster;

import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NetUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Follower 侧：任务写接口透明转发到 Leader。
 *
 * @author wuji
 * @version 1.0.0
 */
public class ClusterWriteProxyFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(ClusterWriteProxyFilter.class);

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 120_000;

    private final ClusterService clusterService;
    private final ClusterWriteProxyService clusterWriteProxyService;

    public ClusterWriteProxyFilter(ClusterService clusterService, ClusterWriteProxyService clusterWriteProxyService) {
        this.clusterService = clusterService;
        this.clusterWriteProxyService = clusterWriteProxyService;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (!shouldForward(request)) {
            filterChain.doFilter(request, response);
            return;
        }
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()
                || StringUtil.equals("anonymousUser", String.valueOf(authentication.getPrincipal()))) {
            filterChain.doFilter(request, response);
            return;
        }
        String leaderBase = clusterService.getLeaderHttpUrl();
        if (StringUtil.isBlank(leaderBase)) {
            writeJson(response, RestResult.restFail("当前无可用 Leader，请稍后重试"));
            return;
        }
        try {
            String roleCode = joinAuthorities(authentication.getAuthorities());
            String ticket = clusterWriteProxyService.issueForLeader(authentication.getName(), roleCode);
            forward(request, response, leaderBase, ticket);
        } catch (Exception e) {
            logger.error("转发写请求到 Leader 失败: {}", e.getMessage(), e);
            writeJson(response, RestResult.restFail("转发到 Leader 失败: " + e.getMessage()));
        }
    }

    private boolean shouldForward(HttpServletRequest request) {
        if (clusterService.isStandalone() || clusterService.isLeader()) {
            return false;
        }
        if (StringUtil.isNotBlank(request.getHeader(ClusterWriteProxyPaths.PROXY_HEADER))) {
            return false;
        }
        return ClusterWriteProxyPaths.shouldProxy(request.getMethod(), request.getServletPath());
    }

    private void forward(HttpServletRequest request, HttpServletResponse response, String leaderBase, String ticket)
            throws IOException {
        StringBuilder url = new StringBuilder(leaderBase);
        url.append(request.getRequestURI());
        if (StringUtil.isNotBlank(request.getQueryString())) {
            url.append('?').append(request.getQueryString());
        }
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url.toString()).openConnection();
            NetUtil.applyInsecureSslIfNeeded(connection);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestProperty(ClusterWriteProxyPaths.PROXY_HEADER, ticket);
            copyRequestHeaders(request, connection);
            byte[] body = resolveBody(request);
            if (body.length > 0) {
                connection.setDoOutput(true);
                if (StringUtil.isBlank(connection.getRequestProperty("Content-Type"))) {
                    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
                }
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(body);
                    out.flush();
                }
            }
            int code = connection.getResponseCode();
            response.setStatus(code);
            copyResponseHeaders(connection, response);
            InputStream in = code >= 400 ? connection.getErrorStream() : connection.getInputStream();
            if (in != null) {
                try (InputStream stream = in; OutputStream out = response.getOutputStream()) {
                    copy(stream, out);
                    out.flush();
                }
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void copyRequestHeaders(HttpServletRequest request, HttpURLConnection connection) {
        Enumeration<String> names = request.getHeaderNames();
        if (names == null) {
            return;
        }
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            if (StringUtil.isBlank(name)) {
                continue;
            }
            String lower = name.toLowerCase();
            if ("host".equals(lower) || "content-length".equals(lower) || "cookie".equals(lower)
                    || "connection".equals(lower) || "transfer-encoding".equals(lower)
                    || ClusterWriteProxyPaths.PROXY_HEADER.equalsIgnoreCase(name)) {
                continue;
            }
            Enumeration<String> values = request.getHeaders(name);
            while (values != null && values.hasMoreElements()) {
                connection.addRequestProperty(name, values.nextElement());
            }
        }
    }

    private void copyResponseHeaders(HttpURLConnection connection, HttpServletResponse response) {
        for (int i = 0; ; i++) {
            String key = connection.getHeaderFieldKey(i);
            String value = connection.getHeaderField(i);
            if (key == null && value == null) {
                break;
            }
            if (key == null) {
                continue;
            }
            String lower = key.toLowerCase();
            if ("transfer-encoding".equals(lower) || "content-length".equals(lower)
                    || "connection".equals(lower) || "set-cookie".equals(lower)) {
                continue;
            }
            response.addHeader(key, value);
        }
    }

    private byte[] resolveBody(HttpServletRequest request) throws IOException {
        String contentType = request.getContentType();
        if (contentType != null && contentType.toLowerCase().contains("application/x-www-form-urlencoded")) {
            return rebuildFormBody(request);
        }
        return readBody(request);
    }

    private byte[] readBody(HttpServletRequest request) throws IOException {
        try (InputStream in = request.getInputStream(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            copy(in, out);
            return out.toByteArray();
        }
    }

    /**
     * 按参数表重建表单 body（兼容容器已解析或未解析两种情况）。
     */
    private byte[] rebuildFormBody(HttpServletRequest request) throws IOException {
        Map<String, String[]> params = request.getParameterMap();
        if (params == null || params.isEmpty()) {
            return new byte[0];
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String[]> entry : params.entrySet()) {
            String key = entry.getKey();
            String[] values = entry.getValue();
            if (values == null) {
                continue;
            }
            for (String value : values) {
                if (sb.length() > 0) {
                    sb.append('&');
                }
                sb.append(URLEncoder.encode(key, StandardCharsets.UTF_8.name()));
                sb.append('=');
                sb.append(URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8.name()));
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
    }

    private String joinAuthorities(Collection<? extends GrantedAuthority> authorities) {
        if (CollectionUtils.isEmpty(authorities)) {
            return StringUtil.EMPTY;
        }
        return authorities.stream().map(GrantedAuthority::getAuthority).collect(Collectors.joining(","));
    }

    private void writeJson(HttpServletResponse response, RestResult result) throws IOException {
        response.setContentType("application/json;charset=utf-8");
        response.setStatus(result.getStatus() > 0 ? result.getStatus() : 200);
        PrintWriter out = response.getWriter();
        out.write(JsonUtil.objToJson(result));
        out.flush();
    }
}
