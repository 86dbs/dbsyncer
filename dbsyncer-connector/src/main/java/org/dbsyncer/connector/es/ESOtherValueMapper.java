package org.dbsyncer.connector.es;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.postgresql.util.PGobject;

import java.util.Map;

/**
 * @author moyu
 * @version 1.0.0
 * @date 2023/10/12 0:07
 */
public class ESOtherValueMapper extends AbstractValueMapper<Map> {

    @Override
    protected Map convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof String) {
            return JsonUtil.jsonToObj((String) val, Map.class);
        }

        if (val instanceof byte[]) {
            return JsonUtil.jsonToObj(new String((byte[]) val), Map.class);
        }

        if (val instanceof PGobject) {
            PGobject pgObject = (PGobject) val;
            return JsonUtil.jsonToObj(pgObject.getValue(), Map.class);
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}