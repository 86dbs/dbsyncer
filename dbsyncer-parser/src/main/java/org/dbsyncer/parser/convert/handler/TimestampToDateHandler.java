package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Date;

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
            value = Date.from(t.toLocalDateTime().atZone(ZoneId.systemDefault()).toInstant());
        }
        return value;
    }

}