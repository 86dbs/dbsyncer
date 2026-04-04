/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.NestedType;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author wuji
 * @Version 1.0.0
 * @Date 2026-04-02 14:37
 */
public class HttpRelTableType extends NestedType {
    @Override
    protected Object merge(Object val, Field field) {
        return val;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        Set<String> types = new HashSet<>();
        types.add(getType().name());
        return types;
    }
}
