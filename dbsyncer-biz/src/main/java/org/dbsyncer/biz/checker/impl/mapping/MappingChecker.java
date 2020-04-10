package org.dbsyncer.biz.checker.impl.mapping;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.biz.util.CheckerTypeUtil;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.constant.ModelConstant;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class MappingChecker extends AbstractChecker implements ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    private Map<String, MappingConfigChecker> map;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(MappingConfigChecker.class);
    }

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        logger.info("checkAddConfigModel mapping params:{}", params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String sourceConnectorId = params.get("sourceConnectorId");
        String targetConnectorId = params.get("targetConnectorId");
        Assert.hasText(name, "mapping name is empty.");
        Assert.hasText(sourceConnectorId, "mapping sourceConnectorId is empty.");
        Assert.hasText(targetConnectorId, "mapping targetConnectorId is empty.");

        Mapping mapping = new Mapping();
        mapping.setName(name);
        mapping.setType(ConfigConstant.MAPPING);
        mapping.setSourceConnectorId(sourceConnectorId);
        mapping.setTargetConnectorId(targetConnectorId);
        mapping.setModel(ModelConstant.FULL);
        mapping.setListener(new ListenerConfig(ListenerEnum.TIMING.getCode()));

        // 修改基本配置
        this.modifyConfigModel(mapping, params);
        return mapping;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("checkEditConfigModel mapping params:{}", params);
        Assert.notEmpty(params, "MappingChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = manager.getMapping(id);
        Assert.notNull(mapping, "Can not find mapping.");

        // 修改基本配置
        this.modifyConfigModel(mapping, params);

        // 同步方式(仅支持全量或增量同步方式)
        String model = params.get("model");
        if (StringUtils.isNotBlank(model)) {
            if (StringUtils.equals(ModelConstant.FULL, model) || StringUtils.equals(ModelConstant.INCREMENT, model)) {
                mapping.setModel(model);
            }
        }

        // 全量配置
        String threadNum = params.get("threadNum");
        mapping.setThreadNum(NumberUtils.toInt(threadNum, mapping.getThreadNum()));
        String batchNum = params.get("batchNum");
        mapping.setBatchNum(NumberUtils.toInt(batchNum, mapping.getBatchNum()));
        
        // 增量配置(日志/定时)
        String incrementStrategy = params.get("incrementStrategy");
        Assert.hasText(incrementStrategy, "MappingChecker check params incrementStrategy is empty");
        String type = CheckerTypeUtil.getCheckerType(incrementStrategy);
        MappingConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(mapping, params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(mapping, params);

        // 增量配置
        return mapping;
    }

}