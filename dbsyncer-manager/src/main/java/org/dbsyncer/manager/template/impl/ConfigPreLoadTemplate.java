package org.dbsyncer.manager.template.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.template.PreLoadTemplate;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
public class ConfigPreLoadTemplate {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private ConfigOperationTemplate operationTemplate;

    public void execute(PreLoadTemplate template) {
        Query query = new Query();
        String filterType = template.filterType();
        query.put(ConfigConstant.CONFIG_MODEL_TYPE, filterType);
        List<Map> list = storageService.queryConfig(query);
        boolean empty = CollectionUtils.isEmpty(list);
        logger.info("PreLoad {}:{}", filterType, empty ? 0 : list.size());
        if (!empty) {
            list.forEach(map -> preload(template, map));
        }
    }

    private void preload(PreLoadTemplate template, Map map) {
        ConfigModel model = template.parseModel((String) map.get(ConfigConstant.CONFIG_MODEL_JSON));
        if (null != model) {
            operationTemplate.save(model, template);
        }
    }

}