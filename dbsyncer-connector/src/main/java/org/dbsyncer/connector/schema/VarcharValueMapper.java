package org.dbsyncer.connector.schema;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class VarcharValueMapper extends AbstractValueMapper<String> {

    @Override
    protected Object convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }

        if (val instanceof LocalDateTime) {
            return ((LocalDateTime) val).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

        if (val instanceof LocalDate) {
            return ((LocalDate) val).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }

        if (val instanceof Date) {
            return DateFormatUtil.dateToString((Date) val);
        }

        throw new ConnectorException(String.format("VarcharValueMapper can not find type [%s], val [%s]", val.getClass(), val));
    }
}