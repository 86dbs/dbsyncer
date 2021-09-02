package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.Timestamp;

/**
 * Long转Timestamp
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/9/2 23:04
 */
public class LongToTimestampHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof Long) {
            Long l = (Long) value;
            value = new Timestamp(l);
        }
        return value;
    }

}