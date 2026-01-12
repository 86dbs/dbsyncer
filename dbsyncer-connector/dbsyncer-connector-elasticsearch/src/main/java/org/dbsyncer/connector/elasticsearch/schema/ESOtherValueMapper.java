/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.elasticsearch.ElasticsearchException;
import org.postgresql.util.PGobject;

import java.util.Map;

/**
 * ES异构字段值转换
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-25 23:10
 */
@Deprecated
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

        throw new ElasticsearchException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}