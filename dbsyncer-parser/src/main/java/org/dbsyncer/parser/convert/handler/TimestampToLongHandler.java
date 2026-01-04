package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.Timestamp;

/**
 * Timestamp转Long
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/9/2 23:04
 */
public class TimestampToLongHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value, java.util.Map<String, Object> row) {
        // row 参数未使用
        if (value instanceof Timestamp) {
            Timestamp t = (Timestamp) value;
            value = t.getTime();
        }
        return value;
    }

}