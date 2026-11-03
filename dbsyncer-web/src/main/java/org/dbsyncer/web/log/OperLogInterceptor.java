/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.log;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.web.util.RequestUtil;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 读取方法上 {@link OperLog}，按模块类型写入操作日志。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class OperLogInterceptor implements HandlerInterceptor {

    @Resource
    private LogService logService;

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        if (!(handler instanceof HandlerMethod)) {
            return;
        }
        String method = request.getMethod();
        if (!"POST".equalsIgnoreCase(method) && !"PUT".equalsIgnoreCase(method)
                && !"DELETE".equalsIgnoreCase(method) && !"PATCH".equalsIgnoreCase(method)) {
            return;
        }
        OperLog ann = ((HandlerMethod) handler).getMethodAnnotation(OperLog.class);
        if (ann == null) {
            return;
        }
        try {
            writeLog(request, response, ann);
        } catch (Exception ignore) {
            // 写日志失败不影响主流程
        }
    }

    private void writeLog(HttpServletRequest request, HttpServletResponse response, OperLog ann) {
        LogType logType = LogType.findByType(ann.type());
        if (logType == null) {
            logType = LogType.SystemLog.INFO;
        }

        ContentCachingRequestWrapper cachedReq = asCachedRequest(request);
        ContentCachingResponseWrapper cachedResp = asCachedResponse(request);
        HttpServletRequest paramRequest = cachedReq != null ? cachedReq : request;

        Map<String, Object> requestParam = OperLogSupport.collectRequestParams(paramRequest);
        Object responseParam = OperLogSupport.collectResponseParam(cachedResp);
        int status = OperLogSupport.resolveStatus(responseParam, response.getStatus());

        String path = OperLogSupport.normalizePath(request);
        String message = resolveMessage(ann, logType, requestParam);
        long start = request.getAttribute(OperLogSupport.ATTR_START_MS) instanceof Long
                ? (Long) request.getAttribute(OperLogSupport.ATTR_START_MS) : System.currentTimeMillis();
        long costMs = Math.max(0L, System.currentTimeMillis() - start);

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("message", message);
        detail.put("requestUri", request.getMethod() + " " + path);
        detail.put("requestParam", requestParam);
        detail.put("responseParam", responseParam);
        detail.put("costMs", costMs);

        logService.log(logType, resolveUsername(), RequestUtil.getClientIp(request), status, detail);
    }

    private String resolveMessage(OperLog ann, LogType logType, Map<String, Object> requestParam) {
        String message = ann.message();
        if (StringUtil.isBlank(message)) {
            message = logType.getMessage() + logType.getName();
        }
        Object name = requestParam.get(ConfigConstant.CONFIG_MODEL_NAME);
        if (name != null && StringUtil.isNotBlank(String.valueOf(name))) {
            return message + ":" + name;
        }
        return message;
    }

    private ContentCachingRequestWrapper asCachedRequest(HttpServletRequest request) {
        Object attr = request.getAttribute(OperLogSupport.ATTR_CACHED_REQUEST);
        if (attr instanceof ContentCachingRequestWrapper) {
            return (ContentCachingRequestWrapper) attr;
        }
        if (request instanceof ContentCachingRequestWrapper) {
            return (ContentCachingRequestWrapper) request;
        }
        return null;
    }

    private ContentCachingResponseWrapper asCachedResponse(HttpServletRequest request) {
        Object attr = request.getAttribute(OperLogSupport.ATTR_CACHED_RESPONSE);
        if (attr instanceof ContentCachingResponseWrapper) {
            return (ContentCachingResponseWrapper) attr;
        }
        return null;
    }

    private String resolveUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()) {
            return StringUtil.EMPTY;
        }
        String name = authentication.getName();
        return "anonymousUser".equals(name) ? StringUtil.EMPTY : StringUtil.getIfBlank(name, StringUtil.EMPTY);
    }
}
