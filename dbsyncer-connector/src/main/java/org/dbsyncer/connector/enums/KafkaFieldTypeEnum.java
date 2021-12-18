package org.dbsyncer.connector.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

/**
 * Kafka字段类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/08 21:13
 * @date 2021/12/17 0:02
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

    private String code;
    private Class clazz;
    private int type;

    KafkaFieldTypeEnum(String code, Class clazz, int type) {
        this.code = code;
        this.clazz = clazz;
        this.type = type;
    }

    public static Class getType(String code) throws ConnectorException {
        for (KafkaFieldTypeEnum e : KafkaFieldTypeEnum.values()) {
            if (StringUtil.equals(e.getCode(), code)) {
                return e.getClazz();
            }
        }
        throw new ConnectorException(String.format("Unsupported code: %s", code));
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