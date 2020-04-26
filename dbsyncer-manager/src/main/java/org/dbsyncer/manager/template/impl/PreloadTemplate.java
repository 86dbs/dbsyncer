package org.dbsyncer.manager.template.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.config.PreloadCallBack;
import org.dbsyncer.manager.config.PreloadConfig;
import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.enums.HandlerEnum;
import org.dbsyncer.manager.template.AbstractTemplate;
import org.dbsyncer.manager.template.Handler;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 预加载配置模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class PreloadTemplate extends AbstractTemplate implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private StorageService storageService;

    @Autowired
    private OperationTemplate operationTemplate;

    public void execute(PreloadConfig config) {
        Query query = new Query();
        String filterType = config.getFilterType();
        query.put(ConfigConstant.CONFIG_MODEL_TYPE, filterType);
        List<Map> list = storageService.queryConfig(query);
        boolean empty = CollectionUtils.isEmpty(list);
        logger.info("PreLoad {}:{}", filterType, empty ? 0 : list.size());
        if (!empty) {
            Handler handler = config.getHandler();
            GroupStrategyEnum strategy = getDefaultStrategy(config);
            list.forEach(map -> {
                String json = (String) map.get(ConfigConstant.CONFIG_MODEL_JSON);
                ConfigModel model = (ConfigModel) handler.execute(new PreloadCallBack(parser, json));
                if (null != model) {
                    operationTemplate.cache(model, strategy);
                }
            });
        }
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        // Load connectors
        execute(new PreloadConfig(ConfigConstant.CONNECTOR, HandlerEnum.PRELOAD_CONNECTOR.getHandler()));
        // Load mappings
        execute(new PreloadConfig(ConfigConstant.MAPPING, HandlerEnum.PRELOAD_MAPPING.getHandler()));
        // Load tableGroups
        execute(new PreloadConfig(ConfigConstant.TABLE_GROUP, GroupStrategyEnum.TABLE, HandlerEnum.PRELOAD_TABLE_GROUP.getHandler()));
        // Load metas
        execute(new PreloadConfig(ConfigConstant.META, HandlerEnum.PRELOAD_META.getHandler()));

    }

}