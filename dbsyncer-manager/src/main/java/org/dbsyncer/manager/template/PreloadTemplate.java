package org.dbsyncer.manager.template;

import com.alibaba.fastjson.JSONObject;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.command.PreloadCommand;
import org.dbsyncer.manager.enums.CommandEnum;
import org.dbsyncer.manager.model.OperationConfig;
import org.dbsyncer.manager.model.QueryConfig;
import org.dbsyncer.manager.template.OperationTemplate.Group;
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

import java.util.Arrays;
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
public final class PreloadTemplate implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private Manager manager;

    @Autowired
    private StorageService storageService;

    @Autowired
    private OperationTemplate operationTemplate;

    public void execute(CommandEnum commandEnum) {
        Query query = new Query();
        query.setType(StorageEnum.CONFIG);
        String modelType = commandEnum.getModelType();
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
            data.forEach(map -> {
                String json = (String) map.get(ConfigConstant.CONFIG_MODEL_JSON);
                ConfigModel model = (ConfigModel) commandEnum.getCommandExecutor().execute(new PreloadCommand(parser, json));
                if (null != model) {
                    operationTemplate.cache(model, commandEnum.getGroupStrategyEnum());
                }
            });
            total += paging.getTotal();
            pageNum++;
        }
        logger.info("PreLoad {}:{}", modelType, total);
    }

    public void reload(String json) {
        Map<String, JSONObject> map = JsonUtil.jsonToObj(json, Map.class);
        if (CollectionUtils.isEmpty(map)) {
            return;
        }

        // Load configs
        reload(map, CommandEnum.PRELOAD_CONFIG);
        // Load connectors
        reload(map, CommandEnum.PRELOAD_CONNECTOR);
        // Load mappings
        reload(map, CommandEnum.PRELOAD_MAPPING);
        // Load metas
        reload(map, CommandEnum.PRELOAD_META);
        // Load projectGroups
        reload(map, CommandEnum.PRELOAD_PROJECT_GROUP);
        launch();
    }

    private void reload(Map<String, JSONObject> map, CommandEnum commandEnum) {
        reload(map, commandEnum, commandEnum.getModelType());
    }

    private void reload(Map<String, JSONObject> map, CommandEnum commandEnum, String groupId) {
        JSONObject config = map.get(groupId);
        Group group = JsonUtil.jsonToObj(config.toJSONString(), Group.class);
        if (null == group) {
            return;
        }

        List<String> index = group.getIndex();
        if (CollectionUtils.isEmpty(index)) {
            return;
        }

        for (String e : index) {
            JSONObject m = map.get(e);
            ConfigModel model = (ConfigModel) commandEnum.getCommandExecutor().execute(new PreloadCommand(parser, m.toJSONString()));
            operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, commandEnum.getGroupStrategyEnum()));
            // Load tableGroups
            if (CommandEnum.PRELOAD_MAPPING == commandEnum) {
                commandEnum = CommandEnum.PRELOAD_TABLE_GROUP;
                reload(map, commandEnum, operationTemplate.getGroupId(model, commandEnum.getGroupStrategyEnum()));
            }
        }
    }

    private void launch() {
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

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        // Load configModels
        Arrays.stream(CommandEnum.values()).filter(commandEnum -> commandEnum.isPreload()).forEach(commandEnum -> execute(commandEnum));

        // Load plugins
        manager.loadPlugins();

        // Check connectors status
        manager.checkAllConnectorStatus();

        // Launch drivers
        launch();
    }

}