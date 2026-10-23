/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * ACL / 业务分组可用的资源类型（编码与 {@link ConfigConstant} 一致）。
 *
 * @author wuji
 * @version 1.0.0
 */
public enum ResourceTypeEnum {

    /**
     * 业务分组（组内叶子继承；不可作为 grantOwner 目标）
     */
    GROUP(ConfigConstant.GROUP, "业务分组", false),

    /**
     * 连接器
     */
    CONNECTOR(ConfigConstant.CONNECTOR, "连接器", true),

    /**
     * 同步任务
     */
    MAPPING(ConfigConstant.MAPPING, "同步任务", true),

    /**
     * 订正校验任务
     */
    VALIDATE_SYNC(ConfigConstant.VALIDATE_SYNC, "订正校验", true),

    /**
     * 整库迁移任务
     */
    DATABASE_SYNC(ConfigConstant.DATABASE_SYNC, "整库迁移", true);

    private final String code;
    private final String name;
    /**
     * 是否叶子资源（可直授；可作为创建者自动授权目标）
     */
    private final boolean leaf;

    ResourceTypeEnum(String code, String name, boolean leaf) {
        this.code = code;
        this.name = name;
        this.leaf = leaf;
    }

    /**
     * 按编码解析；未知返回 {@code null}。
     */
    public static ResourceTypeEnum get(String code) {
        if (StringUtil.isBlank(code)) {
            return null;
        }
        for (ResourceTypeEnum type : values()) {
            if (StringUtil.equals(type.code, code)) {
                return type;
            }
        }
        return null;
    }

    /**
     * 是否支持的资源类型编码。
     */
    public static boolean isSupported(String code) {
        return get(code) != null;
    }

    /**
     * 是否叶子资源编码（不含分组）。
     */
    public static boolean isLeaf(String code) {
        ResourceTypeEnum type = get(code);
        return type != null && type.leaf;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public boolean isLeaf() {
        return leaf;
    }

    public boolean isGroup() {
        return this == GROUP;
    }
}
