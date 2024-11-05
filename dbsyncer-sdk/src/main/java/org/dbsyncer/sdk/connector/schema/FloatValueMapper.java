package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class FloatValueMapper extends AbstractValueMapper<Float> {

    @Override
    protected Float convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Number) {
            Number number = (Number) val;
            return number.floatValue();
        }
        if (val instanceof String) {
            String strVal = (String) val;
            return Float.parseFloat(strVal);
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}