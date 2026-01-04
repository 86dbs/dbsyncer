package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

/**
 * 去掉首字符
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class RemStrFirstHandler extends AbstractHandler {

    @Override
    protected Object convert(String args, Object value, java.util.Map<String, Object> row) {
        // row 参数未使用
        return StringUtil.substring(String.valueOf(value), 1);
    }
}