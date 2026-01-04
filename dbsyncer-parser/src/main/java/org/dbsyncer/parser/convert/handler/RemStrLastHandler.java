package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.Handler;

import java.util.Map;

/**
 * 去掉尾字符
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class RemStrLastHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        String s = String.valueOf(value);
        return StringUtil.substring(s, 0, s.length() - 1);
    }
}