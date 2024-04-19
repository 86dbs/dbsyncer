package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.CustomType;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class CharValueMapper extends AbstractValueMapper<String> {

    @Override
    protected String convert(ConnectorInstance connectorInstance, Object val) {
        if (null != val) {
            if (val instanceof  Integer) {
                return  String.valueOf(val);
            }
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}