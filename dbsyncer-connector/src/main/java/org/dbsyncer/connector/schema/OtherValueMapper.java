package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.spi.ConnectorMapper;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/9/16 16:54
 */
public class OtherValueMapper extends AbstractValueMapper<Object> {

    @Override
    protected Object convert(ConnectorMapper connectorMapper, Object val) {
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}