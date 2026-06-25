/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.IntType;

import java.util.Collections;
import java.util.Set;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public final class MongoDBIntType extends IntType {

    @Override
    public Set<String> getSupportedTypeName() {
        return Collections.singleton("int");
    }

    @Override
    protected Integer merge(Object val, Field field) {
        if (val instanceof String) {
            return Integer.parseInt((String) val);
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1 : 0;
        }
        return throwUnsupportedException(val, field);
    }
}
