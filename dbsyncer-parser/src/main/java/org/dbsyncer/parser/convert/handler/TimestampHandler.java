package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * 系统时间戳
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:03
 */
public class TimestampHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        return new Timestamp(Instant.now().toEpochMilli());
    }
}