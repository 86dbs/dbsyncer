/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.web.model.OpenApiRequest;
import org.dbsyncer.web.model.OpenApiResponse;
import org.dbsyncer.common.util.CryptoUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.biz.impl.IpWhitelistManager;
import org.dbsyncer.web.security.JwtUtil;
import org.dbsyncer.web.security.TimestampValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
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
    
    // 是否公网场景（可通过配置或请求头判断）
    private static final String PUBLIC_NETWORK_HEADER = "X-Public-Network";
    
    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private JwtSecretManager jwtSecretManager;

    @Resource
    private IpWhitelistManager ipWhitelistManager;

    @Resource
    private PreloadTemplate preloadTemplate;

    // TODO HMAC密钥（内网场景使用，可以从配置中获取）
    private String hmacSecret = "dbsyncer-internal-hmac-secret-key-2026";

    @Override
    public boolean preHandle(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull Object handler) throws Exception {
        try {
            // 1. 排除认证接口（登录、刷新token）- 这些接口不需要Token验证
            // TODO 安全问题
            String requestPath = request.getRequestURI();
            if (isAuthEndpoint(requestPath)) {
                return true;
            }

            // 2. 配置是否开启
            if (!preloadTemplate.isPreloadCompleted()) {
                writeErrorResponse(response, 503, "服务暂不可用");
                return false;
            }
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (!systemConfig.isEnableOpenAPI()) {
                writeErrorResponse(response, 500, "未开放API");
                return false;
            }

            // 3. 验证IP白名单（优先验证，避免无效请求消耗资源）
            // 所有OpenAPI接口都需要验证IP白名单，包括登录接口
            String clientIp = getClientIp(request);
            if (!ipWhitelistManager.isAllowed(clientIp)) {
                logger.warn("IP {} 不在白名单中，拒绝访问 {}", clientIp, requestPath);
                writeErrorResponse(response, 403, "IP地址不在白名单中");
                return false;
            }
            
            // 4. 验证Token
            String token = extractToken(request);
            if (StringUtil.isBlank(token)) {
                writeErrorResponse(response, 401, "Token不能为空");
                return false;
            }
            // 获取JWT密钥（支持密钥轮换，尝试当前密钥和上一个密钥）
            String[] jwtSecrets = jwtSecretManager.getSecretsForVerification();
            JwtUtil.TokenInfo tokenInfo = null;
            for (String jwtSecret : jwtSecrets) {
                tokenInfo = JwtUtil.verifyToken(token, jwtSecret);
                if (tokenInfo != null && tokenInfo.isValid()) {
                    break;
                }
            }
            if (tokenInfo == null || !tokenInfo.isValid()) {
                writeErrorResponse(response, 401, "Token无效或已过期");
                return false;
            }
            // 将token信息存储到request attribute中
            request.setAttribute("tokenInfo", tokenInfo);
            
            // 5. 解析加密请求（如果是加密接口）
            if (isEncryptedEndpoint(requestPath)) {
                String requestBody = readRequestBody(request);
                if (StringUtil.isBlank(requestBody)) {
                    writeErrorResponse(response, 400, "请求体不能为空");
                    return false;
                }
                
                try {
                    // 先解析为OpenApiRequest格式（包含时间戳和nonce）
                    OpenApiRequest encryptedRequest = JsonUtil.jsonToObj(requestBody, OpenApiRequest.class);
                    
                    // 验证时间戳和Nonce（在解密之前验证，避免无效请求消耗资源）
                    if (encryptedRequest.getTimestamp() != null && 
                        StringUtil.isNotBlank(encryptedRequest.getNonce())) {
                        if (!TimestampValidator.validate(encryptedRequest.getTimestamp(), encryptedRequest.getNonce())) {
                            writeErrorResponse(response, 400, "时间戳或Nonce验证失败");
                            return false;
                        }
                    }
                    
                    // 判断是否为公网场景
                    boolean isPublicNetwork = isPublicNetwork(request);
                    
                    // 解析加密请求（解密数据）
                    String decryptedData = CryptoUtil.parseEncryptedRequest(
                            requestBody,
                            systemConfig.getRsaConfig().getPrivateKey(),
                            systemConfig.getRsaConfig().getPublicKey(),
                            hmacSecret,
                            isPublicNetwork,
                            String.class
                    );
                    
                    // 将解密后的数据存储到request attribute中，供Controller使用
                    request.setAttribute("decryptedData", decryptedData);
                } catch (Exception e) {
                    logger.error("解析加密请求失败", e);
                    writeErrorResponse(response, 400, "解析加密请求失败: " + e.getMessage());
                    return false;
                }
            }
            
            return true;
        } catch (Exception e) {
            logger.error("OpenAPI拦截器处理失败", e);
            writeErrorResponse(response, 500, "请求处理失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 判断是否为认证接口
     */
    private boolean isAuthEndpoint(String path) {
        return path.endsWith("/openapi/auth/login") || 
               path.endsWith("/openapi/auth/refresh");
    }

    /**
     * 判断是否为加密接口
     */
    private boolean isEncryptedEndpoint(String path) {
        // 数据同步接口需要加密
        return path.contains("/openapi/data/");
    }

    /**
     * 判断是否为公网场景
     */
    private boolean isPublicNetwork(HttpServletRequest request) {
        String header = request.getHeader(PUBLIC_NETWORK_HEADER);
        return "true".equalsIgnoreCase(header);
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
     * 读取请求体
     */
    private String readRequestBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
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
