/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.IntType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLIntType extends IntType {

    @Override
    public Set<String> getSupportedTypeName() {
        // YEAR类型由MySQLYearType处理，不再由本类处理
        return new HashSet<>(Arrays.asList("MEDIUMINT", "INT", "INTEGER"));
    }

    @Override
    protected Integer merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

}