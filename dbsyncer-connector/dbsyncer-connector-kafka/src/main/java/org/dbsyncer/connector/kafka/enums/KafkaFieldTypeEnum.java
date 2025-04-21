/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.enums;

import org.apache.kafka.common.KafkaException;
import org.dbsyncer.common.util.StringUtil;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Kafka字段类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/23 23:13
 */
public enum KafkaFieldTypeEnum {

    // 字符类型
    STRING("String", String.class, Types.VARCHAR),

    // 数字类型
    INTEGER("Integer", Integer.class, Types.INTEGER),
    LONG("Long", Long.class, Types.BIGINT),
    SHORT("Short", Short.class, Types.SMALLINT),
    FLOAT("Float", Float.class, Types.FLOAT),
    DOUBLE("Double", Double.class, Types.DOUBLE),
    BOOLEAN("Boolean", Boolean.class, Types.BIT),

    // 字节类型
//    BINARY("byte[]", Byte.class, Types.BINARY),

    // 日期类型
    DATE("Date", Date.class, Types.DATE),
    TIME("Time", Time.class, Types.TIME),
    TIMESTAMP("Timestamp", Timestamp.class, Types.TIMESTAMP);

    private final String code;
    private final Class clazz;
    private final int type;

    KafkaFieldTypeEnum(String code, Class clazz, int type) {
        this.code = code;
        this.clazz = clazz;
        this.type = type;
    }

    public static Class getType(String code) throws KafkaException {
        for (KafkaFieldTypeEnum e : KafkaFieldTypeEnum.values()) {
            if (StringUtil.equals(e.getCode(), code)) {
                return e.getClazz();
            }
        }
        throw new KafkaException(String.format("Unsupported code: %s", code));
    }

    public String getCode() {
        return code;
    }

    public Class getClazz() {
        return clazz;
    }

    public int getType() {
        return type;
    }
}