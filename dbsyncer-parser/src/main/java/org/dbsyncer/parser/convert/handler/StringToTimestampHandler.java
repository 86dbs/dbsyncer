package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.column.Lexer;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

import java.text.ParseException;

/**
 * 字符串转Timestamp
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/12 23:04
 */
public class StringToTimestampHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) throws ParseException {
        if (value instanceof String) {
            String s = (String) value;
            // 2020-7-12 00:00:00
            if (s.length() < 19) {
                return DateFormatUtil.stringToTimestamp(format(s));
            }

            // 2022-07-21T05:35:34.000+0800
            if (s.length() == 28) {
                return DateFormatUtil.stringToTimestamp(s, DateFormatUtil.GMT_FORMATTER);
            }

            // 2022-07-21T05:35:34.000+08:00
            if (s.length() == 29) {
                s = s.replaceAll(":[^:]*$", "00");
                return DateFormatUtil.stringToTimestamp(s, DateFormatUtil.GMT_FORMATTER);
            }
        }
        return value;
    }

    private String format(String s) {
        StringBuilder buf = new StringBuilder();
        Lexer lexer = new Lexer(s);
        char comma = '-';
        // 年
        nextToken(lexer, buf, comma);
        // 月
        nextToken(lexer, buf, comma);
        // 日
        comma = ' ';
        nextToken(lexer, buf, comma);
        // 时
        comma = ':';
        nextToken(lexer, buf, comma);
        // 分
        nextToken(lexer, buf, comma);
        // 秒
        nextToken(lexer, buf, comma, false);
        return buf.toString();
    }

    private void nextToken(Lexer lexer, StringBuilder buf, char comma) {
        nextToken(lexer, buf, comma, true);
    }

    private void nextToken(Lexer lexer, StringBuilder buf, char comma, boolean appendComma) {
        buf.append(fillZero(lexer.nextToken(comma)));
        if (appendComma) {
            buf.append(comma);
        }
    }

    private String fillZero(String s) {
        if (s.length() < 2) {
            return String.format("%02d", Integer.parseInt(s));
        }
        return s;
    }
}