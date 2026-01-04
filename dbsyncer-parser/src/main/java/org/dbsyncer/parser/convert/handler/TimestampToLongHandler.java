package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Timestamp转Long
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/9/2 23:04
 */
public class TimestampToLongHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            Timestamp t = (Timestamp) value;
            return t.getTime();
        }
        return value;
    }

}