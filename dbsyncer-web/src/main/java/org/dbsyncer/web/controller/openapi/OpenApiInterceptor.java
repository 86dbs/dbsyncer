/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.impl.IpWhitelistManager;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.web.model.OpenApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.UrlPathHelper;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * OpenAPI拦截器
 * 处理token验证和加密解密
 * 
 * @author 穿云
 * @version 1.0.0
 */
@Component
public class OpenApiInterceptor implements HandlerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(OpenApiInterceptor.class);
    
    // Token请求头名称
    private static final String TOKEN_HEADER = "Authorization";
    private static final String TOKEN_PREFIX = "Bearer ";
    
    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private JwtSecretManager jwtSecretManager;

    @Resource
    private IpWhitelistManager ipWhitelistManager;

    @Resource
    private PreloadTemplate preloadTemplate;

    private final UrlPathHelper urlPathHelper = new UrlPathHelper();

    @Override
    public boolean preHandle(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull Object handler) throws Exception {
        try {
            // 1. 配置是否开启
            if (!preloadTemplate.isPreloadCompleted()) {
                writeErrorResponse(response, OpenApiErrorCode.SERVICE_UNAVAILABLE, "服务暂不可用");
                return false;
            }
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (!systemConfig.isEnableOpenAPI()) {
                writeErrorResponse(response, OpenApiErrorCode.NOT_FOUND, "未开放API");
                return false;
            }

            // 2. 验证IP白名单（优先验证，避免无效请求消耗资源）
            // 所有OpenAPI接口都需要验证IP白名单，包括登录接口
            String clientIp = getClientIp(request);
            // 获取请求路径，避免用户请求/.;之类的字符绕过权限判断，导致绕过权限检查风险。
            String requestPath = urlPathHelper.getLookupPathForRequest(request);
            if (!ipWhitelistManager.isAllowed(systemConfig.getIpWhitelistConfig(), clientIp)) {
                logger.warn("IP {} 不在白名单中，拒绝访问 {}", clientIp, requestPath);
                writeErrorResponse(response, OpenApiErrorCode.FORBIDDEN, "IP地址不在白名单中");
                return false;
            }

            // 3. 排除认证接口（登录、刷新token）
            if (isAuthEndpoint(requestPath)) {
                return true;
            }
            
            // 4. 验证Token
            String token = extractToken(request);
            if (!jwtSecretManager.verifyToken(token)) {
                writeErrorResponse(response, OpenApiErrorCode.UNAUTHORIZED, "Token无效或已过期");
                return false;
            }
            
            return true;
        } catch (Exception e) {
            logger.error("OpenAPI拦截器处理失败", e);
            writeErrorResponse(response, OpenApiErrorCode.INTERNAL_ERROR, "请求处理失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 判断是否为认证接口
     */
    private boolean isAuthEndpoint(String path) {
        return path.endsWith("/openapi/auth/login") || path.endsWith("/openapi/auth/refresh");
    }

    /**
     * 获取客户端真实IP地址
     * 考虑代理和负载均衡的情况
     * 
     * @param request HTTP请求
     * @return 客户端IP地址
     */
    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (StringUtil.isNotBlank(ip) && !"unknown".equalsIgnoreCase(ip)) {
            // 多次反向代理后会有多个IP值，第一个IP才是真实IP
            int index = ip.indexOf(',');
            if (index != -1) {
                return ip.substring(0, index).trim();
            } else {
                return ip.trim();
            }
        }

        ip = request.getHeader("X-Real-IP");
        if (StringUtil.isNotBlank(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip.trim();
        }

        ip = request.getHeader("Proxy-Client-IP");
        if (StringUtil.isNotBlank(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip.trim();
        }

        ip = request.getHeader("WL-Proxy-Client-IP");
        if (StringUtil.isNotBlank(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip.trim();
        }

        ip = request.getHeader("HTTP_CLIENT_IP");
        if (StringUtil.isNotBlank(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip.trim();
        }

        ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        if (StringUtil.isNotBlank(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip.trim();
        }

        // 如果以上都获取不到，使用request.getRemoteAddr()
        return request.getRemoteAddr();
    }

    /**
     * 从请求头中提取Token
     */
    private String extractToken(HttpServletRequest request) {
        String authHeader = request.getHeader(TOKEN_HEADER);
        if (StringUtil.isNotBlank(authHeader) && authHeader.startsWith(TOKEN_PREFIX)) {
            return authHeader.substring(TOKEN_PREFIX.length());
        }
        return null;
    }

    /**
     * 写入错误响应
     */
    private void writeErrorResponse(HttpServletResponse response, int code, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json;charset=UTF-8");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        
        OpenApiResponse<Object> errorResponse = OpenApiResponse.fail(code, message);
        String json = JsonUtil.objToJson(errorResponse);
        
        response.getWriter().write(json);
        response.getWriter().flush();
    }
}
