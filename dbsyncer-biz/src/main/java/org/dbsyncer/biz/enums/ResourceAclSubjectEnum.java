/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.enums;

/**
 * 资源 ACL 授权主体类型：0-USER / 1-ROLE。
 *
 * @author wuji
 * @version 1.0.0
 */
public enum ResourceAclSubjectEnum {

    /**
     * 用户
     */
    USER(0, "用户"),

    /**
     * 角色
     */
    ROLE(1, "角色");

    private final int code;
    private final String name;

    ResourceAclSubjectEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按编码解析；未知返回 {@code null}。
     */
    public static ResourceAclSubjectEnum get(int code) {
        for (ResourceAclSubjectEnum subject : values()) {
            if (subject.code == code) {
                return subject;
            }
        }
        return null;
    }

    /**
     * 是否支持的主体类型编码。
     */
    public static boolean isSupported(int code) {
        return get(code) != null;
    }

    public int getCode() {
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
