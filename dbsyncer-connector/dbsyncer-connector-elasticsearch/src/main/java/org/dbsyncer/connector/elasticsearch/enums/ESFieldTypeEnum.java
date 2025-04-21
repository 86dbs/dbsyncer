/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.enums;

import org.dbsyncer.common.util.StringUtil;
import org.elasticsearch.ElasticsearchException;

import java.sql.Types;

/**
 * ES字段类型
 *
 * @author AE86
 * @version 1.0.0
 * @Date 2023-08-26 21:13
 */
public enum ESFieldTypeEnum {

    // 字符类型
    KEYWORD("keyword", Types.VARCHAR),
    TEXT("text", Types.LONGVARCHAR),
    /**
     * ES 5.X之后不再支持string, 由text或keyword取代
     */
    @Deprecated
    STRING("string", Types.VARCHAR),

    // 数字类型
    /**
     * 源库中的类型为unsigned int，建议使用long
     */
    INTEGER("integer", Types.INTEGER),
    /**
     * bit只有一位，建议使用boolean
     */
    LONG("long", Types.BIGINT),
    /**
     * 源库中的类型为unsigned tinyint 或 unsigned smallint，建议使用integer
     */
    SHORT("short", Types.TINYINT),
    BYTE("byte", Types.BIT),
    /**
     * 为保证精度，建议使用text
     */
    DOUBLE("double", Types.DOUBLE),
    FLOAT("float", Types.FLOAT),
    HALF_FLOAT("half_float", Types.FLOAT),
    SCALED_FLOAT("scaled_float", Types.FLOAT),
    BOOLEAN("boolean", Types.BOOLEAN),

    // 日期类型
    /**
     * 默认格式："strict_date_optional_time||epoch_millis"
     * "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"  “2021/01/01 12:10:30” or “2021-01-01”
     */
    DATE("date", Types.DATE),

    // 范围类型
    INTEGER_RANGE("integer_range", Types.INTEGER),
    FLOAT_RANGE("float_range", Types.FLOAT),
    LONG_RANGE("long_range", Types.BIGINT),
    DOUBLE_RANGE("double_range", Types.DOUBLE),
    DATE_RANGE("date_range", Types.DATE),

    // 其他类型
    /**
     * 弥补object类型不足，格式出现list放object会变为array："test": [{"a":"b},{}]
     */
    NESTED("nested", Types.OTHER),
    OBJECT("object", Types.VARCHAR),
    IP("ip", Types.VARCHAR),
    TOKEN_COUNT("token_count", Types.BIGINT),
    GEO_POINT("geo_point", Types.VARCHAR),
    GEO_SHAPE("geo_shape", Types.VARCHAR),
    BINARY("binary", Types.BINARY);

    private final String code;
    private final int type;

    ESFieldTypeEnum(String code, int type) {
        this.code = code;
        this.type = type;
    }

    public static int getType(String code) throws ElasticsearchException {
        for (ESFieldTypeEnum e : ESFieldTypeEnum.values()) {
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