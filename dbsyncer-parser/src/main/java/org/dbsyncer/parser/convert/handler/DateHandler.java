package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.sql.Date;
import java.time.LocalDate;

/**
 * 系统日期
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:03
 */
public class DateHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        return Date.valueOf(LocalDate.now());
    }
}
