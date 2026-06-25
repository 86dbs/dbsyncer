/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Collections;
import java.util.Set;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksBooleanType extends BooleanType {

    @Override
    public Set<String> getSupportedTypeName() {
        return Collections.singleton("BOOLEAN");
    }

    @Override
    protected Boolean merge(Object val, Field field) {
        return (Boolean) convert(val, field);
    }
}
