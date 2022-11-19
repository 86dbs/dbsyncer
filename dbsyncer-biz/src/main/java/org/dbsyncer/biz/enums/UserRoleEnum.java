package org.dbsyncer.biz.enums;

import org.dbsyncer.common.util.StringUtil;

/**
 * 用户角色枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/18 23:21
 */
public enum UserRoleEnum {

    /**
     * 管理员
     */
    ADMIN("admin", "管理员"),

    /**
     * 普通用户
     */
    USER("user", "普通用户");

    private String code;

    private String name;

    UserRoleEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 是否是管理员
     *
     * @param roleCode
     * @return
     */
    public static boolean isAdmin(String roleCode) {
        return ADMIN.getCode().equals(roleCode);
    }

    /**
     * 获取角色名称
     *
     * @param roleCode
     * @return
     */
    public static String getNameByCode(String roleCode) {
        for (UserRoleEnum u : UserRoleEnum.values()) {
            if (StringUtil.equals(roleCode, u.getCode())) {
                return u.getName();
            }
        }
        return "";
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}