/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 参数键枚举
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025/01/08
 */
public enum ParamKeyEnum {

    // 数据表相关参数
    DISABLE_AUTO_CREATE("table.disableAutoCreate", "禁用自动创建", "boolean", "当目标数据表缺失时禁止自动创建"),

    // Kafka 相关参数
    TOPIC("topic", "消息主题", "String", "设置Kafka消息主题名称");

    private final String key;
    private final String name;
    private final String type;
    private final String description;

    ParamKeyEnum(String key, String name, String type, String description) {
        this.key = key;
        this.name = name;
        this.type = type;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    /**
     * 根据key获取枚举
     */
    public static ParamKeyEnum getByKey(String key) {
        for (ParamKeyEnum param : values()) {
            if (param.getKey().equals(key)) {
                return param;
            }
        }
        return null;
    }
}