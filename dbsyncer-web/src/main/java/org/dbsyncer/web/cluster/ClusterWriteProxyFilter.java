/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.cluster;

import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 无全局 Leader，写请求在本节点执行，不再转发。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public class ClusterWriteProxyFilter extends OncePerRequestFilter {

    public ClusterWriteProxyFilter(ClusterService clusterService, ClusterWriteProxyService clusterWriteProxyService) {
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        filterChain.doFilter(request, response);
    }
}
