/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.enums;

/**
 * 资源 ACL 权限档位：0-只读 / 1-可读写（含启停）。
 *
 * @author wuji
 * @version 1.0.0
 */
public enum ResourceAclPermissionEnum {

    /**
     * 只读
     */
    READ(0, "只读"),

    /**
     * 可读写（含启停）
     */
    READ_WRITE(1, "可读写");

    private final int code;
    private final String name;

    ResourceAclPermissionEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按编码解析；未知返回 {@code null}。
     */
    public static ResourceAclPermissionEnum get(int code) {
        for (ResourceAclPermissionEnum permission : values()) {
            if (permission.code == code) {
                return permission;
            }
        }
        return null;
    }

    /**
     * 是否支持的权限编码。
     */
    public static boolean isSupported(int code) {
        return get(code) != null;
    }

    /**
     * 已授权档位是否满足动作要求：read 需任意档；write/operate 需可读写。
     */
    public static boolean satisfies(int grantedPermission, String actionCode) {
        ResourceAclActionEnum action = ResourceAclActionEnum.get(actionCode);
        if (action == null) {
            return false;
        }
        if (action == ResourceAclActionEnum.READ) {
            return grantedPermission == READ.code || grantedPermission == READ_WRITE.code;
        }
        return grantedPermission == READ_WRITE.code;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
