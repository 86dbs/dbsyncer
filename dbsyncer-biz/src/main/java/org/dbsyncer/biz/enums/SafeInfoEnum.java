package org.dbsyncer.biz.enums;

/**
 * 安全信息枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/26 23:21
 */
public enum SafeInfoEnum {

    /**
     * 密码
     */
    PASSWORD("password");

    private String code;

    SafeInfoEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

}