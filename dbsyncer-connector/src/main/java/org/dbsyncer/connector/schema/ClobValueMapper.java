package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;

import java.sql.Clob;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class ClobValueMapper extends AbstractValueMapper<Clob> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.CLOB || val instanceof byte[] || val instanceof String;
    }

    @Override
    protected Clob convert(ConnectorMapper connectorMapper, Object val) {
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}