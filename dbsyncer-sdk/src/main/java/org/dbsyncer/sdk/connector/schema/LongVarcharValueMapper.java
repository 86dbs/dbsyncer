package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.sql.Date;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class LongVarcharValueMapper extends AbstractValueMapper<String> {

    @Override
    protected String convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        if (val instanceof Date) {
            return String.valueOf(val);
        }
        if (val instanceof Integer) {
            return String.valueOf(val);
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}