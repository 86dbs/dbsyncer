package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.AbstractHandler;
import org.springframework.util.Assert;

/**
 * 从前面截取N个字符
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class SubStrFirstHandler extends AbstractHandler {

    @Override
    protected Object convert(String args, Object value, java.util.Map<String, Object> row) {
        // row 参数未使用
        Assert.isTrue(NumberUtil.isCreatable(args), "参数必须为正整数.");
        String s = String.valueOf(value);
        int size = NumberUtil.toInt(args);
        return StringUtil.substring(s, 0, size);
    }
}