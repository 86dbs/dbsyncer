/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.StringUtil;

/**
 * 任务类型枚举
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-03-22 19:52
 */
public enum CommonTaskTypeEnum {

    /**
     * 同步任务类型
     */
    MAPPING("mapping", "org.dbsyncer.parser.model.Mapping"),

    /**
     * 订正校验
     */
    VALIDATE_SYNC("validateSync", "org.dbsyncer.sdk.model.ValidateSyncTask"),

    /**
     * 整库迁移
     */
    DATABASE_SYNC("databaseSync", "org.dbsyncer.sdk.model.DatabaseSyncTask");

    /**
     * 配置类型 code（与 {@code ConfigModel#type} / 存储 TYPE 列一致）
     */
    private final String code;

    /**
     * 对应 ConfigModel 实现类
     */
    private final Class<? extends ConfigModel> clazz;

    CommonTaskTypeEnum(String code, String className) {
        this.code = code;
        this.clazz = loadClass(className);
    }

    /**
     * 按名称或 code 解析任务类型。
     *
     * @param typeStr 任务类型字符串（枚举名或驼峰 code）
     * @return 任务类型枚举；无法识别返回 null
     */
    public static CommonTaskTypeEnum parse(String typeStr) {
        if (StringUtil.isBlank(typeStr)) {
            return null;
        }
        for (CommonTaskTypeEnum e : values()) {
            if (StringUtil.equals(typeStr, e.code)) {
                return e;
            }
        }
        return null;
    }

    /**
     * 导入配置包专用：将历史大写 TYPE 规范为当前 code；运行时路径勿依赖。
     *
     * @param typeStr 导入包中的 type
     * @return 规范后的 code；空白原样返回
     */
    public static String normalizeImportType(String typeStr) {
        if (StringUtil.isBlank(typeStr)) {
            return typeStr;
        }
        if (StringUtil.equals(typeStr, "VALIDATE_SYNC")) {
            return VALIDATE_SYNC.code;
        }
        if (StringUtil.equals(typeStr, "DATABASE_SYNC")) {
            return DATABASE_SYNC.code;
        }
        return typeStr;
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends ConfigModel> loadClass(String className) {
        try {
            return (Class<? extends ConfigModel>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("任务类型实现类不存在: " + className, e);
        }
    }

    public String getCode() {
        return code;
    }

    public Class<? extends ConfigModel> getClazz() {
        return clazz;
    }

}
