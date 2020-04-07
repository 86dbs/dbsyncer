package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.MappingLogConfigChecker;
import org.dbsyncer.listener.config.LogListenerConfig;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * 检查增量MYSQL配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/4/7 16:17
 */
@Component
public class MysqlLogConfigChecker implements MappingLogConfigChecker {

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        LogListenerConfig listenerConfig = new LogListenerConfig();
        String label = params.get("incrementStrategyLogTableLabel");
        Assert.hasText(label, "MysqlLogConfigChecker check params incrementStrategyLogTableLabel is empty");
        listenerConfig.setTableLabel(label);
        mapping.setListener(listenerConfig);
    }

}