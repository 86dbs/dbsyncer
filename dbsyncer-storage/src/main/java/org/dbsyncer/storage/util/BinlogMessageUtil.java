package org.dbsyncer.storage.util;

import com.google.protobuf.ByteString;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import oracle.sql.STRUCT;
import oracle.sql.TIMESTAMP;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.schema.CustomData;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.binlog.BinlogColumnValue;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.enums.BinlogByteEnum;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Objects;

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
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/14 22:07
 */
public abstract class BinlogMessageUtil {

    private static final Logger logger = LoggerFactory.getLogger(BinlogMessageUtil.class);

    public static BinlogMap toBinlogMap(Map<String, Object> data) {
        BinlogMap.Builder dataBuilder = BinlogMap.newBuilder();
        data.forEach((k, v) -> {
            if (null != v) {
                ByteString bytes = serializeValue(v);
                if (null != bytes) {
                    dataBuilder.putRow(k, bytes);
                }
            }
        });
        return dataBuilder.build();
    }

    public static ByteString serializeValue(Object v) {
        // 自定义数据类型
        if (v instanceof CustomData) {
            CustomData cd = (CustomData) v;
            return ByteString.copyFromUtf8(cd.toString());
        }
        // Map、List 及其实现（LinkedHashMap、TreeMap、ArrayList 等）统一按 JSON 存储，避免落 default 不序列化
        if (v instanceof Map || v instanceof List) {
            return ByteString.copyFromUtf8(JsonUtil.objToJsonSafe(v));
        }

        String type = v.getClass().getName();
        switch (type) {
            // 字节
            case "[B":
                return ByteString.copyFrom((byte[]) v);
            case "java.lang.Byte":
                return ByteString.copyFromUtf8(String.valueOf(v));

            // 字符串
            case "java.lang.String":
                return ByteString.copyFromUtf8((String) v);
            case "org.postgresql.util.PGobject":
                PGobject pgObject = (PGobject) v;
                return ByteString.copyFromUtf8(Objects.requireNonNull(pgObject.getValue()));
            case "org.postgresql.geometric.PGpoint":
                PGpoint pgpoint = (PGpoint) v;
                return ByteString.copyFromUtf8(Objects.requireNonNull(pgpoint.getValue()));
            case "java.util.UUID":
                UUID uuid = (UUID) v;
                return ByteString.copyFromUtf8(uuid.toString());

            // 时间
            case "java.sql.Timestamp":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> {
                    Timestamp timestamp = (Timestamp) v;
                    buffer.putLong(timestamp.getTime());
                });
            case "java.sql.Date":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> {
                    Date date = (Date) v;
                    buffer.putLong(date.getTime());
                });
            case "java.util.Date":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> {
                    java.util.Date uDate = (java.util.Date) v;
                    buffer.putLong(uDate.getTime());
                });
            case "java.sql.Time":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> {
                    Time time = (Time) v;
                    buffer.putLong(time.getTime());
                });

            // 数字
            case "java.lang.Integer":
                return allocateByteBufferToByteString(BinlogByteEnum.INTEGER, buffer -> buffer.putInt((Integer) v));
            case "java.math.BigInteger":
                BigInteger bigInteger = (BigInteger) v;
                return ByteString.copyFrom(bigInteger.toByteArray());
            case "java.lang.Long":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> buffer.putLong((Long) v));
            case "java.lang.Short":
                return allocateByteBufferToByteString(BinlogByteEnum.SHORT, buffer -> buffer.putShort((Short) v));
            case "java.lang.Float":
                return allocateByteBufferToByteString(BinlogByteEnum.FLOAT, buffer -> buffer.putFloat((Float) v));
            case "java.lang.Double":
                return allocateByteBufferToByteString(BinlogByteEnum.DOUBLE, buffer -> buffer.putDouble((Double) v));
            case "java.math.BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) v;
                return ByteString.copyFromUtf8(bigDecimal.toString());
            case "java.util.BitSet":
                BitSet bitSet = (BitSet) v;
                return ByteString.copyFrom(bitSet.toByteArray());

            // 布尔(1为true;0为false)
            case "java.lang.Boolean":
                return allocateByteBufferToByteString(BinlogByteEnum.SHORT, buffer -> {
                    Boolean b = (Boolean) v;
                    buffer.putShort((short) (b ? 1 : 0));
                });
            case "java.time.LocalDateTime":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> buffer.putLong(Timestamp.valueOf((LocalDateTime) v).getTime()));
            case "oracle.sql.TIMESTAMP":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer -> {
                    TIMESTAMP timeStamp = (TIMESTAMP) v;
                    try {
                        buffer.putLong(timeStamp.timestampValue().getTime());
                    } catch (SQLException e) {
                        logger.error(e.getMessage());
                    }
                });
            case "oracle.sql.BLOB":
                return ByteString.copyFrom(getBytes((BLOB) v));
            case "oracle.sql.CLOB":
                return ByteString.copyFrom(getBytes((CLOB) v));
            case "oracle.sql.STRUCT":
                return ByteString.copyFrom(getBytes((STRUCT) v));
            default:
                logger.error("Unsupported serialize value type:{}", type);
                return null;
        }
    }

    public static Object deserializeValue(int type, ByteString v) {
        BinlogColumnValue value = new BinlogColumnValue(v);
        if (value.isNull()) {
            return null;
        }

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
                return null;
        }
    }

    /**
     * 判断 ByteString 是否按定长二进制存储。若为 JSON 串等（如 date_range、*_range 的 Map），
     * 应按 asString 反序列化，否则按 LONG/INT 等会误读前 N 字节导致日期或数字乱码。
     */
    private static boolean isStoredAsFixed(ByteString v, int expectedBytes) {
        return v != null && v.size() == expectedBytes;
    }

    private static boolean isStoredAsLong(ByteString v) {
        return isStoredAsFixed(v, BinlogByteEnum.LONG.getByteLength());
    }

    private static ByteString allocateByteBufferToByteString(BinlogByteEnum byteType, ByteStringMapper mapper) {
        ByteBuffer buffer = ByteBuffer.allocate(byteType.getByteLength());
        mapper.apply(buffer);
        buffer.flip();
        return ByteString.copyFrom(buffer, byteType.getByteLength());
    }

    private static byte[] getBytes(BLOB blob) {
        InputStream is = null;
        byte[] b = null;
        try {
            is = blob.getBinaryStream();
            b = new byte[(int) blob.length()];
            int read = is.read(b);
            if (-1 == read) {
                return b;
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(is);
        }
        return b;
    }

    private static byte[] getBytes(CLOB clob) {
        try {
            long length = clob.length();
            if (length > 0) {
                return clob.getSubString(1, (int) length).getBytes(Charset.defaultCharset());
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return new byte[0];
    }

    private static byte[] getBytes(STRUCT v) {
        try {
            return v.toBytes();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new StorageException(e);
        }
    }

    interface ByteStringMapper {
        void apply(ByteBuffer buffer);
    }

}