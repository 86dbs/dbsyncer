package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.Handler;
import org.dbsyncer.parser.util.ExpressionUtil;

import java.util.Map;

/**
 * 表达式处理器
 *
 * @author DBSyncer
 * @version 1.0.0
 */
public class ExpressionHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        if (StringUtil.isBlank(args) || row == null) {
            return null;
        }
        // 表达式不依赖原值，使用 row 计算表达式
        return ExpressionUtil.evaluate(args, row);
    }
}

