/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema;

import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.elasticsearch.ElasticsearchException;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * ES日期字段值转换
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-25 23:10
 */
public class ESDateValueMapper extends AbstractValueMapper<java.util.Date> {

    @Override
    protected java.util.Date convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            return new java.util.Date(timestamp.getTime());
        }

        if (val instanceof Date) {
            Date date = (Date) val;
            return new java.util.Date(date.getTime());
        }

        if (val instanceof Long) {
            return new java.util.Date((Long) val);
        }

        if (val instanceof Integer) {
            return new java.util.Date(Long.parseLong(val.toString()));
        }

        throw new ElasticsearchException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}