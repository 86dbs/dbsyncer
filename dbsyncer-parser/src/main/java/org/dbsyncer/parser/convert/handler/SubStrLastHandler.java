package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.convert.Handler;

import java.util.Map;
import org.springframework.util.Assert;

/**
 * 从后面截取N个字符
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class SubStrLastHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        try {
            Assert.isTrue(NumberUtil.isCreatable(args), "参数必须为正整数.");
            String s = String.valueOf(value);
            int size = NumberUtil.toInt(args);
            int length = s.length();
            return StringUtil.substring(s, length - size, length);
        } catch (Exception e) {
            throw new ParserException(e.getMessage());
        }
    }
}