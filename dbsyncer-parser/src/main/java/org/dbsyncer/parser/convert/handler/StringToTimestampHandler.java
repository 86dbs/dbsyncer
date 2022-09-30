package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

/**
 * 字符串转Timestamp
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/12 23:04
 */
public class StringToTimestampHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof String) {
            String s = (String) value;
            return DateFormatUtil.stringToTimestamp(s);
        }
        return value;
    }

}