package org.dbsyncer.connector.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Timestamp;
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
    STRING("String", String.class),

    // 数字类型
    INTEGER("Integer", Integer.class),
    LONG("Long", Long.class),
    SHORT("Short", Short.class),
    DOUBLE("Double", Double.class),
    FLOAT("Float", Float.class),
    BOOLEAN("Boolean", Boolean.class),
    BYTE("Byte", Byte.class),

    // 日期类型
    DATE("DATE", Date.class),
    TIMESTAMP("TIMESTAMP", Timestamp.class);

    private String code;
    private Class type;

    KafkaFieldTypeEnum(String code, Class type) {
        this.code = code;
        this.type = type;
    }

    public static Class getType(String code) throws ConnectorException {
        for (KafkaFieldTypeEnum e : KafkaFieldTypeEnum.values()) {
            if (StringUtil.equals(e.getCode(), code)) {
                return e.getType();
            }
        }
        throw new ConnectorException(String.format("Unsupported code: %s", code));
    }

    public String getCode() {
        return code;
    }

    public Class getType() {
        return type;
    }

}