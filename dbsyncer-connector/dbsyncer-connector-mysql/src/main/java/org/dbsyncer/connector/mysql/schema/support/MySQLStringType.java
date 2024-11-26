/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:24
 */
public class MySQLStringType extends StringType {
    @Override
    protected String merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected String convert(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected String getDefaultMergedVal() {
        return "";
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}