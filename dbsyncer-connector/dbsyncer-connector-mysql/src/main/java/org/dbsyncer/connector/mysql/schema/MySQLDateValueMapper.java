/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public final class MySQLDateValueMapper extends AbstractValueMapper<Date> {

    @Override
    protected boolean skipConvert(Object val) {
        if (val instanceof String) {
            return StringUtil.equals((CharSequence) val, "0000-00-00");
        }

        // 兼容MySQL年份
        if (val instanceof Integer) {
            return true;
        }
        return super.skipConvert(val);
    }

    @Override
    protected Date convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
        }

        if (val instanceof String) {
            String s = (String) val;
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (null != timestamp) {
                return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
            }
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}