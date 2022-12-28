package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class TimestampValueMapper extends AbstractValueMapper<Timestamp> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.TIMESTAMP;
    }

    @Override
    protected Timestamp convert(ConnectorMapper connectorMapper, Object val) {
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

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}