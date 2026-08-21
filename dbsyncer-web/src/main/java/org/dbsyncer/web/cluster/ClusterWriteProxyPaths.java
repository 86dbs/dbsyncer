/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.cluster;

import org.dbsyncer.common.util.StringUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Follower → Leader 写代理路径白名单（任务 / 连接器等配置写操作）。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class ClusterWriteProxyPaths {

    /**
     * 代理请求头：短时 JWT（载荷同 Web SSO 票据）
     */
    public static final String PROXY_HEADER = "X-Dbsyncer-Cluster-Proxy";

    private static final Set<String> WRITE_PATHS;

    static {
        Set<String> paths = new HashSet<>(Arrays.asList(
                "/connector/add",
                "/connector/edit",
                "/connector/remove",
                "/connector/copy",
                "/database-sync/add",
                "/database-sync/edit",
                "/database-sync/remove",
                "/database-sync/start",
                "/database-sync/stop",
                "/validate-sync/add",
                "/validate-sync/edit",
                "/validate-sync/copy",
                "/validate-sync/remove",
                "/validate-sync/start",
                "/validate-sync/stop",
                "/validate-sync/refreshTables",
                "/validate-sync/refreshFields",
                "/validate-sync/addTableGroup",
                "/validate-sync/editTableGroup",
                "/validate-sync/removeTableGroup",
                "/validate-sync/manualRevise",
                "/mapping/add",
                "/mapping/edit",
                "/mapping/remove",
                "/mapping/start",
                "/mapping/stop",
                "/mapping/copy",
                "/mapping/sync",
                "/mapping/refreshTables",
                "/mapping/saveCustomTable",
                "/mapping/removeCustomTable",
                "/tableGroup/add",
                "/tableGroup/edit",
                "/tableGroup/remove",
                "/tableGroup/refreshFields"
        ));
        WRITE_PATHS = Collections.unmodifiableSet(paths);
    }

    private ClusterWriteProxyPaths() {
    }

    /**
     * 是否为需转发到 Leader 的写接口。
     *
     * @param method HTTP 方法
     * @param path   servlet path（不含 context-path）
     * @return true 需要代理
     */
    public static boolean shouldProxy(String method, String path) {
        if (!StringUtil.equalsIgnoreCase("POST", method) || StringUtil.isBlank(path)) {
            return false;
        }
        String normalized = normalize(path);
        return WRITE_PATHS.contains(normalized);
    }

    private static String normalize(String path) {
        String p = path.trim();
        if (p.length() > 1 && p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }
}
