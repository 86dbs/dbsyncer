/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.enums;

import org.dbsyncer.common.util.StringUtil;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * 资源 ACL 动作。
 *
 * @author wuji
 * @version 1.0.0
 */
public enum ResourceAclActionEnum {

    /**
     * 查看
     */
    READ("read", "查看"),

    /**
     * 编辑
     */
    WRITE("write", "编辑"),

    /**
     * 启停/执行等运行时操作
     */
    OPERATE("operate", "操作");

    private final String code;
    private final String name;

    ResourceAclActionEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按编码解析（大小写不敏感）；未知返回 {@code null}。
     */
    public static ResourceAclActionEnum get(String code) {
        if (StringUtil.isBlank(code)) {
            return null;
        }
        String key = code.trim().toLowerCase(Locale.ROOT);
        for (ResourceAclActionEnum action : values()) {
            if (StringUtil.equals(action.code, key)) {
                return action;
            }
        }
        return null;
    }

    /**
     * 是否支持的动作编码。
     */
    public static boolean isSupported(String code) {
        return get(code) != null;
    }

    /**
     * 全部动作编码（有序）。
     */
    public static Set<String> allCodes() {
        Set<String> codes = new LinkedHashSet<>();
        for (ResourceAclActionEnum action : values()) {
            codes.add(action.code);
        }
        return Collections.unmodifiableSet(codes);
    }

    /**
     * 全部动作编码逗号拼接（如创建者授权）。
     */
    public static String joinAllCodes() {
        return String.join(StringUtil.COMMA, allCodes());
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
