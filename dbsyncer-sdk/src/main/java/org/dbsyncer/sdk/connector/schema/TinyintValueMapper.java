package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class TinyintValueMapper extends AbstractValueMapper<Integer> {

    @Override
    protected Integer convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new Integer(b ? 1 : 0);
        }
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.intValue();
        }
        if (val instanceof String) {
            return NumberUtil.toInt((String) val);
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}