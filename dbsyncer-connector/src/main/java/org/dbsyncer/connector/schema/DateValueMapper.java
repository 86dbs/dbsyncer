package org.dbsyncer.connector.schema;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.spi.ConnectorMapper;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class DateValueMapper extends AbstractValueMapper<Date> {

    @Override
    protected Date convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
        }

        if (val instanceof String) {
            String s = (String) val;
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (null != timestamp) {
                return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
            }
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}