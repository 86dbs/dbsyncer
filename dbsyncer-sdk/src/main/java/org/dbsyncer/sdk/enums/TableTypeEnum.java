/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

import org.dbsyncer.sdk.SdkException;

/**
 * 表类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/08/26 21:13
 */
public enum TableTypeEnum {

    /**
     * 表
     */
    TABLE("TABLE"),

    /**
     * 视图
     */
    VIEW("VIEW"),

    /**
     * 物化视图
     */
    MATERIALIZED_VIEW("MATERIALIZED VIEW"),

    /**
     * SQL
     */
    SQL("SQL"),

    /**
     * ‌半结构化(File，Kafka，Redis 等定义JSON数据格式)
     */
    SEMI_STRUCTURED("SEMI_STRUCTURED");

    private final String code;

    TableTypeEnum(String code) {
        this.code = code;
    }

    /**
     * 是否表类型
     *
     * @param type
     * @return
     */
    public static boolean isTable(String type) {
        return TABLE.getCode().equals(type);
    }

    public static TableTypeEnum getTableType(String type) throws SdkException {
        for (TableTypeEnum e : TableTypeEnum.values()) {
            if (e.getCode().equals(type)) {
                return e;
            }
        }
        throw new SdkException(String.format("TableTypeEnum type \"%s\" does not exist.", type));
    }

    public String getCode() {
        return code;
    }

}