package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.AbstractHandler;
import org.springframework.util.Assert;

import java.sql.Timestamp;

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
            Assert.hasText(args, "缺少pattern参数.");
            String s = (String) value;
            value = DateFormatUtil.stringToTimestamp(s, args);
        }
        return value;
    }

}