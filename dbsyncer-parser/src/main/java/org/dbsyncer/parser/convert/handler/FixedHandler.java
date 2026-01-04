package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.Handler;

import java.util.Map;

/**
 * 固定值处理器
 *
 * @author DBSyncer
 * @version 1.0.0
 */
public class FixedHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // 固定值：直接返回 args，忽略原值和 row
        return StringUtil.isBlank(args) ? null : args;
    }
}

