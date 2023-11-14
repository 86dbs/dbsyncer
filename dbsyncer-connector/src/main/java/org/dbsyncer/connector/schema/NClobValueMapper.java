package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.spi.ConnectorMapper;

import java.sql.NClob;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class NClobValueMapper extends AbstractValueMapper<NClob> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.NCLOB || val instanceof byte[];
    }

    @Override
    protected NClob convert(ConnectorMapper connectorMapper, Object val) {
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}