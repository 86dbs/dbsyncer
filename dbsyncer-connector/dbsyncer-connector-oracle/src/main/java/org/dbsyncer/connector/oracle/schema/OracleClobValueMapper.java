package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.CustomType;

import java.nio.charset.StandardCharsets;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
@Deprecated
public final class OracleClobValueMapper extends AbstractValueMapper<CustomType> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.CLOB || val instanceof String;
    }

    @Override
    protected CustomType convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof byte[]) {
            return new CustomType(new String((byte[]) val, StandardCharsets.UTF_8));
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}