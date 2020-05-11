package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.MappingLogConfigChecker;
import org.dbsyncer.parser.model.ListenerConfig;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * 检查增量DqlOracle配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/4/10 20:57
 */
@Component
public class DqlOracleLogConfigChecker implements MappingLogConfigChecker {

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        ListenerConfig config = mapping.getListener();
        Assert.notNull(config, "ListenerConfig can not be null.");

        String label = params.get("incrementStrategyLogTableLabel");
        Assert.hasText(label, "DqlOracleLogConfigChecker check params incrementStrategyLogTableLabel is empty");
        config.setTableLabel(label);
        mapping.setListener(config);
    }

}