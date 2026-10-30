/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.log;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 为可能写操作日志的请求包装缓存体，供 {@link OperLogInterceptor} 读取；本 Filter 不落库。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
@Order(Ordered.LOWEST_PRECEDENCE - 20)
public class OperLogFilter extends OncePerRequestFilter {

    private static final Set<String> EXCLUDE_PREFIXES = new HashSet<>(Arrays.asList(
            "/login", "/logout", "/openapi/", "/css/", "/js/", "/img/", "/plugins/"
    ));

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String method = request.getMethod();
        if (!"POST".equalsIgnoreCase(method) && !"PUT".equalsIgnoreCase(method)
                && !"DELETE".equalsIgnoreCase(method) && !"PATCH".equalsIgnoreCase(method)) {
            return true;
        }
        String path = OperLogSupport.normalizePath(request);
        for (String prefix : EXCLUDE_PREFIXES) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return path.contains("/page/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        ContentCachingRequestWrapper req = new ContentCachingRequestWrapper(request);
        ContentCachingResponseWrapper resp = new ContentCachingResponseWrapper(response);
        req.setAttribute(OperLogSupport.ATTR_CACHED_REQUEST, req);
        req.setAttribute(OperLogSupport.ATTR_CACHED_RESPONSE, resp);
        req.setAttribute(OperLogSupport.ATTR_START_MS, System.currentTimeMillis());
        try {
            filterChain.doFilter(req, resp);
        } finally {
            resp.copyBodyToResponse();
        }
    }
}
