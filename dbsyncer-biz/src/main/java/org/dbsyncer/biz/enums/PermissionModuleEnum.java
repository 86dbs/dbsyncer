/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.enums;

import org.dbsyncer.common.util.StringUtil;

import java.util.Locale;

/**
 * 功能权限模块（与 {@code dbsyncer_permission.MODULE} 编码一致）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-23
 */
public enum PermissionModuleEnum {

    /**
     * 用户
     */
    USER("user", "用户"),

    /**
     * 连接器
     */
    CONNECTOR("connector", "连接器"),

    /**
     * 同步任务
     */
    MAPPING("mapping", "同步任务"),

    /**
     * 告警通知
     */
    NOTICE("notice", "告警通知"),

    /**
     * 授权许可
     */
    LICENSE("license", "授权许可"),

    /**
     * 系统配置
     */
    SYSTEM("system", "系统配置"),

    /**
     * 资源授权
     */
    ACL("acl", "资源");

    private final String code;
    private final String name;

    PermissionModuleEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按编码解析（大小写不敏感）；未知返回 {@code null}。
     */
    public static PermissionModuleEnum get(String code) {
        if (StringUtil.isBlank(code)) {
            return null;
        }
        String key = code.trim().toLowerCase(Locale.ROOT);
        for (PermissionModuleEnum module : values()) {
            if (StringUtil.equals(module.code, key)) {
                return module;
            }
        }
        return null;
    }

    /**
     * 解析模块展示名；未知编码原样返回，空则「其他」。
     */
    public static String resolveName(String code) {
        if (StringUtil.isBlank(code)) {
            return "其他";
        }
        PermissionModuleEnum module = get(code);
        return module != null ? module.name : code;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
