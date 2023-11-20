package org.dbsyncer.connector.es;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author moyu
 * @version 1.0.0
 * @date 2023/10/12 0:07
 */
public class ESDateValueMapper extends AbstractValueMapper<java.util.Date> {

    @Override
    protected java.util.Date convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            return new java.util.Date(timestamp.getTime());
        }

        if (val instanceof Date) {
            Date date = (Date) val;
            return new java.util.Date(date.getTime());
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}