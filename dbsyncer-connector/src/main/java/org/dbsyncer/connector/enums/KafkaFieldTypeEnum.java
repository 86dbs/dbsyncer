package org.dbsyncer.connector.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Types;

/**
 * Kafka字段类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/08 21:13
 */
public enum KafkaFieldTypeEnum {

    // 字符类型
    STRING("string", Types.VARCHAR),

    // 数字类型
    INTEGER("integer", Types.INTEGER),
    LONG("long", Types.BIGINT),
    SHORT("short", Types.TINYINT),
    DOUBLE("double", Types.DOUBLE),
    FLOAT("float", Types.FLOAT),
    BOOLEAN("boolean", Types.BOOLEAN),
    BYTE("byte", Types.BINARY),

    // 日期类型
    DATE("date", Types.DATE),
    TIMESTAMP("timestamp", Types.TIMESTAMP);

    private String code;
    private int type;

    KafkaFieldTypeEnum(String code, int type) {
        this.code = code;
        this.type = type;
    }

    public static int getType(String code) throws ConnectorException {
        for (KafkaFieldTypeEnum e : KafkaFieldTypeEnum.values()) {
            if (StringUtil.equals(e.getCode(), code)) {
                return e.getType();
            }
        }
        return Types.VARCHAR;
    }

    public String getCode() {
        return code;
    }

    public int getType() {
        return type;
    }

}