/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbaseoracle.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Collections;
import java.util.Set;

/**
 * OceanBase Oracle 模式双精度浮点
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 14:10
 */
public final class OceanBaseOracleDoubleType extends DoubleType {

    @Override
    public Set<String> getSupportedTypeName() {
        return Collections.singleton("BINARY_DOUBLE");
    }

    @Override
    protected Double merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        if (val instanceof String) {
            return Double.valueOf(StringUtil.trimToEmpty(val.toString()));
        }
        return throwUnsupportedException(val, field);
    }
}
