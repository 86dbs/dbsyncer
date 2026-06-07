/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.binlog.BinlogColumnValue;
import org.dbsyncer.common.enums.BinlogByteEnum;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Java语言提供了八种基本类型，六种数字类型（四个整数型，两个浮点型），一种字符类型，一种布尔型。
 * <p>
 * <ol>
 * <li>整数：包括int,short,byte,long</li>
 * <li>浮点型：float,double</li>
 * <li>字符：char</li>
 * <li>布尔：boolean</li>
 * </ol>
 *
 * <pre>
 * 类型     长度     大小      最小值     最大值
 * byte     1Byte    8-bit     -128       +127
 * short    2Byte    16-bit    -2^15      +2^15-1
 * int      4Byte    32-bit    -2^31      +2^31-1
 * long     8Byte    64-bit    -2^63      +2^63-1
 * float    4Byte    32-bit    IEEE754    IEEE754
 * double   8Byte    64-bit    IEEE754    IEEE754
 * char     2Byte    16-bit    Unicode 0  Unicode 2^16-1
 * boolean  8Byte    64-bit
 * </pre>
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2024-11-26 20:50
 */
public abstract class AbstractSchemaResolver implements SchemaResolver {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, DataType> mapping = new ConcurrentHashMap<>();

    public AbstractSchemaResolver() {
        initDataTypeMapping(mapping);
        Assert.notEmpty(mapping, "At least one data type is required.");
    }

    protected abstract void initDataTypeMapping(Map<String, DataType> mapping);

    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        return mapping.get(field.getTypeName());
    }

    @Override
    public Object merge(Object val, Field field) {
        DataType dataType = getDataType(mapping, field);
        if (dataType != null) {
            return dataType.mergeValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] merge into [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convert(Object val, Field field) {
        DataType dataType = getDataType(mapping, field);
        if (dataType != null) {
            return dataType.convertValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public DataTypeEnum getFieldType(Field field) {
        DataType dataType = getDataType(mapping, field);
        if (dataType != null) {
            return dataType.getType();
        }
        throw new SdkException(String.format("%s does not support field type [%s]", getClass().getSimpleName(), field.getTypeName()));
    }

    @Override
    public Object deserialize(ByteString v, Field field) {
        BinlogColumnValue value = new BinlogColumnValue(v);
        if (value.isNull()) {
            return null;
        }
        DataTypeEnum fieldType = getFieldType(field);
        switch (fieldType) {
            // 字符串
            case STRING:
                return value.asString();
            // 时间：仅当为 8 字节时按 LONG 解析，否则按字符串（如 date_range 的 JSON、日期格式串）
            case TIMESTAMP:
                return isStoredAsLong(v) ? value.asTimestamp() : value.asString();
            case TIME:
                return isStoredAsLong(v) ? value.asTime() : value.asString();
            case DATE:
                return isStoredAsLong(v) ? value.asDate() : value.asString();

            // 数字：仅当字节长度与类型匹配时按二进制解析，否则按字符串（如 *_range 的 JSON）
            case INT:
                if (isStoredAsFixed(v, BinlogByteEnum.INTEGER.getByteLength())) {
                    return value.asInteger();
                }
                if (isStoredAsLong(v)) {
                    return value.asLong().intValue();
                }
                return value.asString();
            case SHORT:
                if (isStoredAsFixed(v, BinlogByteEnum.SHORT.getByteLength())) {
                    return value.asShort();
                }
                if (isStoredAsFixed(v, BinlogByteEnum.INTEGER.getByteLength())) {
                    return value.asInteger().shortValue();
                }
                if (isStoredAsLong(v)) {
                    return value.asLong().shortValue();
                }
                return value.asString();
            case LONG:
                if (isStoredAsLong(v)) {
                    return value.asLong();
                }
                if (isStoredAsFixed(v, BinlogByteEnum.INTEGER.getByteLength())) {
                    return value.asInteger().longValue();
                }
                return value.asString();
            case FLOAT:
                return isStoredAsFixed(v, BinlogByteEnum.FLOAT.getByteLength()) ? value.asFloat() : value.asString();
            case DOUBLE:
                return isStoredAsFixed(v, BinlogByteEnum.DOUBLE.getByteLength()) ? value.asDouble() : value.asString();
            case DECIMAL:
                return value.asBigDecimal();
            // 布尔
            case BOOLEAN:
                return value.asBoolean();

            // 字节
            case BYTES:
                return value.asByteArray();
            default:
                break;
        }
        return null;
    }

    @Override
    public ByteString serialize(Object value, Field field) {
        if (value == null) {
            return null;
        }
        if (field == null) {
            return serialize(value);
        }
        Object converted = convert(value, field);
        switch (getFieldType(field)) {
            case INT:
            case SHORT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return serializeByFieldType(converted, getFieldType(field));
            default:
                return serialize(converted);
        }
    }

    private ByteString serialize(Object value) {
        // 自定义数据类型
        if (value instanceof CustomData) {
            CustomData cd = (CustomData) value;
            return ByteString.copyFromUtf8(cd.toString());
        }
        // Map、List 及其实现（LinkedHashMap、TreeMap、ArrayList 等）统一按 JSON 存储，避免落 default 不序列化
        if (value instanceof Map || value instanceof List) {
            return ByteString.copyFromUtf8(JsonUtil.objToJsonSafe(value));
        }

        String type = value.getClass().getName();
        switch (type) {
            // 字节
            case "[B":
                return ByteString.copyFrom((byte[]) value);
            case "java.lang.Byte":
                return ByteString.copyFromUtf8(String.valueOf(value));
            // 字符串
            case "java.lang.String":
                return ByteString.copyFromUtf8((String) value);
            case "java.util.UUID":
                UUID uuid = (UUID) value;
                return ByteString.copyFromUtf8(uuid.toString());
            // 时间
            case "java.sql.Timestamp":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer-> {
                    Timestamp timestamp = (Timestamp) value;
                    buffer.putLong(timestamp.getTime());
                });
            case "java.sql.Date":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer-> {
                    Date date = (Date) value;
                    buffer.putLong(date.getTime());
                });
            case "java.util.Date":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer-> {
                    java.util.Date uDate = (java.util.Date) value;
                    buffer.putLong(uDate.getTime());
                });
            case "java.sql.Time":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer-> {
                    Time time = (Time) value;
                    buffer.putLong(time.getTime());
                });
            // 数字
            case "java.lang.Integer":
                return allocateByteBufferToByteString(BinlogByteEnum.INTEGER, buffer->buffer.putInt((Integer) value));
            case "java.math.BigInteger":
                BigInteger bigInteger = (BigInteger) value;
                return ByteString.copyFrom(bigInteger.toByteArray());
            case "java.lang.Long":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer->buffer.putLong((Long) value));
            case "java.lang.Short":
                return allocateByteBufferToByteString(BinlogByteEnum.SHORT, buffer->buffer.putShort((Short) value));
            case "java.lang.Float":
                return allocateByteBufferToByteString(BinlogByteEnum.FLOAT, buffer->buffer.putFloat((Float) value));
            case "java.lang.Double":
                return allocateByteBufferToByteString(BinlogByteEnum.DOUBLE, buffer->buffer.putDouble((Double) value));
            case "java.math.BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) value;
                return ByteString.copyFromUtf8(bigDecimal.toString());
            case "java.util.BitSet":
                BitSet bitSet = (BitSet) value;
                return ByteString.copyFrom(bitSet.toByteArray());
            // 布尔(1为true;0为false)
            case "java.lang.Boolean":
                return allocateByteBufferToByteString(BinlogByteEnum.SHORT, buffer-> {
                    Boolean b = (Boolean) value;
                    buffer.putShort((short) (b ? 1 : 0));
                });
            case "java.time.LocalDateTime":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer->buffer.putLong(Timestamp.valueOf((LocalDateTime) value).getTime()));
            default:
                logger.error("Unsupported serialize value type:{}", type);
        }
        return null;
    }

    protected ByteString serializeByFieldType(Object value, DataTypeEnum fieldType) {
        if (value == null) {
            return null;
        }
        switch (fieldType) {
            case INT:
                return allocateByteBufferToByteString(BinlogByteEnum.INTEGER, buffer->buffer.putInt(((Number) value).intValue()));
            case SHORT:
                return allocateByteBufferToByteString(BinlogByteEnum.SHORT, buffer->buffer.putShort(((Number) value).shortValue()));
            case LONG:
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer->buffer.putLong(((Number) value).longValue()));
            case FLOAT:
                return allocateByteBufferToByteString(BinlogByteEnum.FLOAT, buffer->buffer.putFloat(((Number) value).floatValue()));
            case DOUBLE:
                return allocateByteBufferToByteString(BinlogByteEnum.DOUBLE, buffer->buffer.putDouble(((Number) value).doubleValue()));
            default:
                return serialize(value);
        }
    }

    protected ByteString allocateByteBufferToByteString(BinlogByteEnum byteType, ByteStringMapper mapper) {
        ByteBuffer buffer = ByteBuffer.allocate(byteType.getByteLength());
        mapper.apply(buffer);
        buffer.flip();
        return ByteString.copyFrom(buffer, byteType.getByteLength());
    }

    /**
     * 判断 ByteString 是否按定长二进制存储。若为 JSON 串等（如 date_range、*_range 的 Map），
     * 应按 asString 反序列化，否则按 LONG/INT 等会误读前 N 字节导致日期或数字乱码。
     */
    protected boolean isStoredAsFixed(ByteString v, int expectedBytes) {
        return v != null && v.size() == expectedBytes;
    }

    protected boolean isStoredAsLong(ByteString v) {
        return isStoredAsFixed(v, BinlogByteEnum.LONG.getByteLength());
    }

}
