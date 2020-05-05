package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.biz.checker.MappingLogConfigChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * 日志配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class LogConfigChecker implements MappingConfigChecker, ApplicationContextAware {

    @Autowired
    private Manager manager;

    private Map<String, MappingLogConfigChecker> map;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(MappingLogConfigChecker.class);
    }

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        String connectorId = mapping.getSourceConnectorId();
        Connector connector = manager.getConnector(connectorId);
        ConnectorConfig config = connector.getConfig();
        String type = StringUtil.toLowerCaseFirstOne(config.getConnectorType()).concat("LogConfigChecker");
        MappingLogConfigChecker checker = map.get(type);
        if (null != checker) {
            checker.modify(mapping, params);
        }
        ListenerConfig listener = mapping.getListener();
        Assert.notNull(listener, "ListenerConfig can not be null.");

        listener.setListenerType(ListenerEnum.LOG.getCode());
    }

}