package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class SmallintValueMapper extends AbstractValueMapper<Integer> {

    @Override
    protected Integer convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Short) {
            Short s = (Short) val;
            return new Integer(s);
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}