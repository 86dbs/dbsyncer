/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.convert.Handler;

import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * 字符串转日期自定义格式
 *
 * @author wuji
 */
public class StringToFormatDateHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof String) {
                String s = (String) value;
                return DateFormatUtil.stringToDate(s, DateTimeFormatter.ofPattern(args));
            }
            return value;
        } catch (ParseException e) {
            throw new ParserException(e.getMessage());
        }
    }
}