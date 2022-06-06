package org.dbsyncer.manager.template.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.config.PreloadCallBack;
import org.dbsyncer.manager.config.PreloadConfig;
import org.dbsyncer.manager.config.QueryConfig;
import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.enums.HandlerEnum;
import org.dbsyncer.manager.template.AbstractTemplate;
import org.dbsyncer.manager.template.Handler;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
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
    private Manager manager;

    @Autowired
    private StorageService storageService;

    @Autowired
    private OperationTemplate operationTemplate;

    public void execute(PreloadConfig config) {
        Query query = new Query();
        query.setType(StorageEnum.CONFIG);
        String modelType = config.getConfigModelType();
        query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, modelType);

        int pageNum = 1;
        int pageSize = 20;
        long total = 0;
        for (; ; ) {
            query.setPageNum(pageNum);
            query.setPageSize(pageSize);
            Paging paging = storageService.query(query);
            List<Map> data = (List<Map>) paging.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }
            Handler handler = config.getHandlerEnum().getHandler();
            GroupStrategyEnum strategy = getDefaultStrategy(config);
            data.forEach(map -> {
                String json = (String) map.get(ConfigConstant.CONFIG_MODEL_JSON);
                ConfigModel model = (ConfigModel) handler.execute(new PreloadCallBack(parser, json));
                if (null != model) {
                    operationTemplate.cache(model, strategy);
                }
            });
            total += paging.getTotal();
            pageNum ++;
        }

        logger.info("PreLoad {}:{}", modelType, total);
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        // Load connectors
        execute(new PreloadConfig(ConfigConstant.CONNECTOR, HandlerEnum.PRELOAD_CONNECTOR));
        // Load mappings
        execute(new PreloadConfig(ConfigConstant.MAPPING, HandlerEnum.PRELOAD_MAPPING));
        // Load tableGroups
        execute(new PreloadConfig(ConfigConstant.TABLE_GROUP, GroupStrategyEnum.TABLE, HandlerEnum.PRELOAD_TABLE_GROUP));
        // Load metas
        execute(new PreloadConfig(ConfigConstant.META, HandlerEnum.PRELOAD_META));
        // Load configs
        execute(new PreloadConfig(ConfigConstant.CONFIG, HandlerEnum.PRELOAD_CONFIG));

        // Load plugins
        manager.loadPlugins();

        // Check connectors status
        manager.checkAllConnectorStatus();

        // Launch drivers
        Meta meta = new Meta();
        meta.setType(ConfigConstant.META);
        QueryConfig<Meta> queryConfig = new QueryConfig<>(meta);
        List<Meta> metas = operationTemplate.queryAll(queryConfig);
        if (!CollectionUtils.isEmpty(metas)) {
            metas.forEach(m -> {
                // 恢复驱动状态
                if (MetaEnum.RUNNING.getCode() == m.getState()) {
                    Mapping mapping = manager.getMapping(m.getMappingId());
                    manager.start(mapping);
                } else if (MetaEnum.STOPPING.getCode() == m.getState()) {
                    manager.changeMetaState(m.getId(), MetaEnum.READY);
                }
            });
        }
    }

}