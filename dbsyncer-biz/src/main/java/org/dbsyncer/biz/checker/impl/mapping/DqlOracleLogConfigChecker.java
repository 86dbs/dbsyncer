package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.MappingLogConfigChecker;
import org.dbsyncer.listener.config.ListenerConfig;
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
        mapping.setListener(config);
    }

}