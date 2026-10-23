/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.enums;

import org.dbsyncer.common.util.StringUtil;

import java.util.Locale;

/**
 * 资源 ACL 授权主体类型。
 *
 * @author wuji
 * @version 1.0.0
 */
public enum ResourceAclSubjectEnum {

    /**
     * 角色
     */
    ROLE("ROLE", "角色"),

    /**
     * 用户
     */
    USER("USER", "用户");

    private final String code;
    private final String name;

    ResourceAclSubjectEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按编码解析（大小写不敏感）；未知返回 {@code null}。
     */
    public static ResourceAclSubjectEnum get(String code) {
        if (StringUtil.isBlank(code)) {
            return null;
        }
        String key = code.trim().toUpperCase(Locale.ROOT);
        for (ResourceAclSubjectEnum subject : values()) {
            if (StringUtil.equals(subject.code, key)) {
                return subject;
            }
        }
        return null;
    }

    /**
     * 是否支持的主体类型编码。
     */
    public static boolean isSupported(String code) {
        return get(code) != null;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public boolean isRole() {
        return this == ROLE;
    }

    public boolean isUser() {
        return this == USER;
    }
}
