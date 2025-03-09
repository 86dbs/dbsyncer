package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.AbstractHandler;
import org.springframework.util.Assert;

/**
 * 替换
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:03
 */
public class ReplaceHandler extends AbstractHandler {

    @Override
    protected Object convert(String args, Object value) {
        Assert.hasText(args, "缺少替换参数.");
        String[] split = StringUtil.split(args, StringUtil.COMMA);
        String a = split[0];
        String b = split.length == 2 ? split[1] : "";
        return StringUtil.replace(String.valueOf(value), a, b);
    }
}