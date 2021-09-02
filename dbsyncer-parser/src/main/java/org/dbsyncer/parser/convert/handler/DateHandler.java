package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.Handler;

import java.util.Date;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:03
 */
public class DateHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        return null == value || StringUtil.isBlank(String.valueOf(value)) ? new Date() : value;
    }
}