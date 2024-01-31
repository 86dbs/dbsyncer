package org.dbsyncer.sdk.connector.schema;

import microsoft.sql.DateTimeOffset;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class TimestampValueMapper extends AbstractValueMapper<Timestamp> {

    @Override
    protected Timestamp convert(ConnectorInstance connectorInstance, Object val) throws SQLException {
        if (val instanceof Date) {
            Date date = (Date) val;
            return new Timestamp(date.getTime());
        }

        if (val instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) val;
            return Timestamp.valueOf(dateTime);
        }

        if (val instanceof String) {
            String s = (String) val;
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (null != timestamp) {
                return timestamp;
            }
        }

        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            String s = new String(bytes);
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (null != timestamp) {
                return timestamp;
            }
        }

        if (val instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) val;
            return new Timestamp(date.getTime());
        }

        if (val instanceof oracle.sql.TIMESTAMP) {
            oracle.sql.TIMESTAMP timestamp = (oracle.sql.TIMESTAMP) val;
            return timestamp.timestampValue();
        }

        if (val instanceof OffsetDateTime) {
            OffsetDateTime date = (OffsetDateTime) val;
            return Timestamp.from(date.toInstant());
        }

        if (val instanceof microsoft.sql.DateTimeOffset) {
            LocalDateTime dateTime = ((DateTimeOffset) val).getOffsetDateTime().toLocalDateTime();
            return Timestamp.valueOf(dateTime);
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}