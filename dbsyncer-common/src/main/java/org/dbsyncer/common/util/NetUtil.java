/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

/**
 * 本机通告地址解析与节点 Web 根地址拼装。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public abstract class NetUtil {

    private NetUtil() {
    }

    /**
     * 拼装节点 Web 根地址（无尾斜杠），可附带 {@code server.servlet.context-path}。
     *
     * @param ip          IP
     * @param port        端口
     * @param ssl         是否 HTTPS（对应 {@code server.ssl.enabled}）
     * @param contextPath 访问上下文路径，如 {@code /}、{@code /dbsyncer}；空或 {@code /} 时不拼接
     * @return 如 {@code http://ip:port/dbsyncer}；非法时为空
     */
    public static String buildWebRootUrl(String ip, int port, boolean ssl, String contextPath) {
        if (StringUtil.isBlank(ip) || port <= 0) {
            return StringUtil.EMPTY;
        }
        StringBuilder url = new StringBuilder();
        url.append(ssl ? "https://" : "http://").append(ip).append(':').append(port);
        String path = normalizeContextPath(contextPath);
        if (StringUtil.isNotBlank(path)) {
            url.append(path);
        }
        return url.toString();
    }

    /**
     * 规范化 context-path：空或 {@code /} 返回空串；否则保证以 {@code /} 开头且无尾斜杠。
     *
     * @param contextPath 原始路径
     * @return 规范化后的路径
     */
    public static String normalizeContextPath(String contextPath) {
        if (StringUtil.isBlank(contextPath)) {
            return StringUtil.EMPTY;
        }
        String path = contextPath.trim();
        if ("/".equals(path)) {
            return StringUtil.EMPTY;
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        while (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * 将应用内相对路径拼到 context-path 后（用于 {@code sendRedirect}）。
     * <p>
     * {@code sendRedirect("/x")} 相对容器根，不含 context-path；须显式拼接。
     *
     * @param contextPath {@link javax.servlet.http.HttpServletRequest#getContextPath()}
     * @param path        应用内路径，如 {@code /login.html}、{@code /}
     * @return 带 context-path 的路径
     */
    public static String joinContextPath(String contextPath, String path) {
        String ctx = normalizeContextPath(contextPath);
        String p = StringUtil.isBlank(path) ? "/" : path.trim();
        if (!p.startsWith("/")) {
            p = "/" + p;
        }
        if (StringUtil.isBlank(ctx)) {
            return p;
        }
        if (p.equals(ctx) || p.startsWith(ctx + "/")) {
            return p;
        }
        return "/".equals(p) ? ctx + "/" : ctx + p;
    }

}
