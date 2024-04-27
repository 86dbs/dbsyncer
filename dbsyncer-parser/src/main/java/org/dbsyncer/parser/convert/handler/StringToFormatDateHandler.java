package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 字符串转日期自定义格式
 *
 * @author wuji
 */
public class StringToFormatDateHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) throws ParseException {

        DateFormat dateFormat = new SimpleDateFormat(args);

        if (value instanceof String) {
            String s = (String) value;
            return DateFormatUtil.stringToTimestamp(s, dateFormat);
        }
        return value;
    }
}