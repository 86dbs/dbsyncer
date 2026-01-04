package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.convert.Handler;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * 替换
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:03
 */
public class ReplaceHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        try {
            Assert.hasText(args, "缺少替换参数.");
            String[] split = StringUtil.split(args, StringUtil.COMMA);
            String a = split[0];
            String b = split.length == 2 ? split[1] : "";
            return StringUtil.replace(String.valueOf(value), a, b);
        } catch (Exception e) {
            throw new ParserException(e.getMessage());
        }
    }
}