/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema;

import com.google.protobuf.ByteString;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import oracle.sql.STRUCT;
import oracle.sql.TIMESTAMP;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.enums.BinlogByteEnum;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.schema.support.OracleBytesType;
import org.dbsyncer.connector.oracle.schema.support.OracleDateType;
import org.dbsyncer.connector.oracle.schema.support.OracleDecimalType;
import org.dbsyncer.connector.oracle.schema.support.OracleDoubleType;
import org.dbsyncer.connector.oracle.schema.support.OracleFloatType;
import org.dbsyncer.connector.oracle.schema.support.OracleIntType;
import org.dbsyncer.connector.oracle.schema.support.OracleLongType;
import org.dbsyncer.connector.oracle.schema.support.OracleStringType;
import org.dbsyncer.connector.oracle.schema.support.OracleTimestampType;
import org.dbsyncer.sdk.schema.AbstractDatabaseSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2024-12-24 23:45
 */
public final class OracleSchemaResolver extends AbstractDatabaseSchemaResolver {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
            new OracleBytesType(),
            new OracleDateType(),
            new OracleDecimalType(),
            new OracleDoubleType(),
            new OracleFloatType(),
            new OracleIntType(),
            new OracleLongType(),
            new OracleStringType(),
            new OracleTimestampType())
        .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
            if (mapping.containsKey(typeName)) {
                throw new OracleException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    public ByteString serialize(Object value) {
        String type = value.getClass().getName();
        switch (type) {
            case "oracle.sql.TIMESTAMP":
                return allocateByteBufferToByteString(BinlogByteEnum.LONG, buffer-> {
                    TIMESTAMP timeStamp = (TIMESTAMP) value;
                    try {
                        buffer.putLong(timeStamp.timestampValue().getTime());
                    } catch (SQLException e) {
                        logger.error(e.getMessage());
                    }
                });
            case "oracle.sql.BLOB":
                return ByteString.copyFrom(getBytes((BLOB) value));
            case "oracle.sql.CLOB":
                return ByteString.copyFrom(getBytes((CLOB) value));
            case "oracle.sql.STRUCT":
                return ByteString.copyFrom(getBytes((STRUCT) value));
            default:
                break;
        }
        return super.serialize(value);
    }

    private byte[] getBytes(BLOB blob) {
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

    private byte[] getBytes(CLOB clob) {
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

    private byte[] getBytes(STRUCT v) {
        try {
            return v.toBytes();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new OracleException(e);
        }
    }
}
