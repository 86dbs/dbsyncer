package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.math.BigDecimal;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class NumberValueMapper extends AbstractValueMapper<BigDecimal> {

    @Override
    protected BigDecimal convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            return new BigDecimal((String) val);
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}