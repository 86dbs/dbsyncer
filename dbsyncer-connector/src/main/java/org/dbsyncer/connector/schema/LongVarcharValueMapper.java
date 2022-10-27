package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Date;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class LongVarcharValueMapper extends AbstractValueMapper<String> {

    @Override
    protected String convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        if (val instanceof Date) {
            return String.valueOf(val);
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}