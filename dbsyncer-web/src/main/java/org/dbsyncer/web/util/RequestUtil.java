/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.util;

import org.dbsyncer.common.util.StringUtil;

import javax.servlet.http.HttpServletRequest;

/**
 * HTTP 请求工具。
 *
 * @author wuji
 * @version 1.0.0
 */
public abstract class RequestUtil {

    /**
     * 获取客户端真实 IP（考虑反向代理）。
     *
     * @param request HTTP 请求
     * @return 客户端 IP，无法解析时返回空串
     */
    public static String getClientIp(HttpServletRequest request) {
        if (request == null) {
            return StringUtil.EMPTY;
        }
        String ip = firstValidIp(request.getHeader("X-Forwarded-For"));
        if (StringUtil.isNotBlank(ip)) {
            int index = ip.indexOf(',');
            return index != -1 ? ip.substring(0, index).trim() : ip.trim();
        }
        ip = firstValidIp(request.getHeader("X-Real-IP"));
        if (StringUtil.isNotBlank(ip)) {
            return ip;
        }
        ip = firstValidIp(request.getHeader("Proxy-Client-IP"));
        if (StringUtil.isNotBlank(ip)) {
            return ip;
        }
        ip = firstValidIp(request.getHeader("WL-Proxy-Client-IP"));
        if (StringUtil.isNotBlank(ip)) {
            return ip;
        }
        ip = firstValidIp(request.getHeader("HTTP_CLIENT_IP"));
        if (StringUtil.isNotBlank(ip)) {
            return ip;
        }
        ip = firstValidIp(request.getHeader("HTTP_X_FORWARDED_FOR"));
        if (StringUtil.isNotBlank(ip)) {
            return ip;
        }
        String remote = request.getRemoteAddr();
        return StringUtil.isBlank(remote) ? StringUtil.EMPTY : remote;
    }

    private static String firstValidIp(String ip) {
        if (StringUtil.isBlank(ip) || "unknown".equalsIgnoreCase(ip)) {
            return StringUtil.EMPTY;
        }
        return ip.trim();
    }
}
