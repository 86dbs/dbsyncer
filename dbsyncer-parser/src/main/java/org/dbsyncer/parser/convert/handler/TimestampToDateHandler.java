package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * 时间戳转日期
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/9/2 23:04
 */
public class TimestampToDateHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof Timestamp) {
            Timestamp t = (Timestamp) value;
            value = new Date(t.getTime());
        }
        return value;
    }

}