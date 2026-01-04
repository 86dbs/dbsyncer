package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

/**
 * 时间戳转日期
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/9/2 23:04
 */
public class TimestampToDateHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            Timestamp t = (Timestamp) value;
            return new Date(t.getTime());
        }
        return value;
    }

}