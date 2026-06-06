/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.binlog.BinlogColumnValue;
import org.dbsyncer.common.enums.BinlogByteEnum;
import org.dbsyncer.sdk.model.Field;

import java.sql.Types;

/**
 * 关系性数据标准解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 23:42
 */
public abstract class AbstractDatabaseSchemaResolver extends AbstractSchemaResolver {

    /**
     * TODO 临时方案，后续将会统一使用field.getTypeName(); 转为标准数据类型 AbstractSchemaResolver#getFieldType(Field)
     *
     * @param v 序列化的值
     * @param field 数据类型
     */
    @Override
    public Object deserialize(ByteString v, Field field) {
        BinlogColumnValue value = new BinlogColumnValue(v);
        if (value.isNull()) {
            return null;
        }

        int type = field.getType();
        switch (type) {
            // 字符串
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.CHAR:
                return value.asString();

            // 时间：仅当为 8 字节时按 LONG 解析，否则按字符串（如 date_range 的 JSON、日期格式串）
            case Types.TIMESTAMP:
                return isStoredAsLong(v) ? value.asTimestamp() : value.asString();
            case Types.TIME:
                return isStoredAsLong(v) ? value.asTime() : value.asString();
            case Types.DATE:
                return isStoredAsLong(v) ? value.asDate() : value.asString();

            // 数字：仅当字节长度与类型匹配时按二进制解析，否则按字符串（如 *_range 的 JSON）
            case Types.INTEGER:
            case Types.TINYINT:
                return isStoredAsFixed(v, BinlogByteEnum.INTEGER.getByteLength()) ? value.asInteger() : value.asString();
            case Types.SMALLINT:
                return isStoredAsFixed(v, BinlogByteEnum.SHORT.getByteLength()) ? value.asShort() : value.asString();
            case Types.BIGINT:
                return isStoredAsLong(v) ? value.asLong() : value.asString();
            case Types.FLOAT:
            case Types.REAL:
                return isStoredAsFixed(v, BinlogByteEnum.FLOAT.getByteLength()) ? value.asFloat() : value.asString();
            case Types.DOUBLE:
                return isStoredAsFixed(v, BinlogByteEnum.DOUBLE.getByteLength()) ? value.asDouble() : value.asString();
            case Types.DECIMAL:
            case Types.NUMERIC:
                return value.asBigDecimal();

            // 布尔
            case Types.BOOLEAN:
                return value.asBoolean();

            // 字节
            case Types.BIT:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NCLOB:
            case Types.CLOB:
            case Types.BLOB:
            case Types.OTHER:
                return value.asByteArray();

            // 暂不支持
            case Types.ROWID:
                return null;
            default:
                break;
        }
        return super.deserialize(v, field);
    }
}
