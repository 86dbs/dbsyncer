package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.postgresql.util.PGobject;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class VarcharValueMapper extends AbstractValueMapper<String> {

    @Override
    protected String convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }

        if (val instanceof Integer) {
            return Integer.toString((Integer) val);
        }

        if (val instanceof Long) {
            return Long.toString((Long) val);
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

        if (val instanceof java.util.Date) {
            return DateFormatUtil.dateToString((java.util.Date) val);
        }

        if (val instanceof PGobject) {
            PGobject pgObject = (PGobject) val;
            return pgObject.getValue();
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}