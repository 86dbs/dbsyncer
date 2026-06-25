/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Set;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public final class MongoDBLongType extends LongType {

    @Override
    public Set<String> getSupportedTypeName() {
        return Collections.singleton("long");
    }

    @Override
    protected Long merge(Object val, Field field) {
        if (val instanceof String) {
            return Long.parseLong((String) val);
        }
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof Timestamp) {
            return ((Timestamp) val).getTime();
        }
        if (val instanceof Date) {
            return ((Date) val).getTime();
        }
        if (val instanceof java.util.Date) {
            return ((java.util.Date) val).getTime();
        }
        return throwUnsupportedException(val, field);
    }
}
