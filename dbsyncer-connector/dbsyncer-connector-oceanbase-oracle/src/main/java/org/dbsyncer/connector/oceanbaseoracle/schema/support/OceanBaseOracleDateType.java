/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbaseoracle.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DateType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Set;

/**
 * OceanBase Oracle 模式 DATE 类型
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 14:10
 */
public final class OceanBaseOracleDateType extends DateType {

    @Override
    public Set<String> getSupportedTypeName() {
        return Collections.singleton("DATE");
    }

    @Override
    protected Date merge(Object val, Field field) {
        if (val instanceof Date) {
            return (Date) val;
        }
        if (val instanceof Timestamp) {
            return new Date(((Timestamp) val).getTime());
        }
        if (val instanceof java.util.Date) {
            return new Date(((java.util.Date) val).getTime());
        }
        if (val instanceof LocalDateTime) {
            return new Date(Timestamp.valueOf((LocalDateTime) val).getTime());
        }
        if (val instanceof String) {
            Timestamp timestamp = DateFormatUtil.stringToTimestamp((String) val);
            if (timestamp != null) {
                return new Date(timestamp.getTime());
            }
            return DateFormatUtil.stringToDate((String) val);
        }
        return throwUnsupportedException(val, field);
    }
}
