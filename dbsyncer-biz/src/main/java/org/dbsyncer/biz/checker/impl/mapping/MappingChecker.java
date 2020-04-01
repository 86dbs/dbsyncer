/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.mapping;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.constant.ModelConstant;
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
    public String checkConfigModel(Map<String, String> params) {
        logger.info("check mapping params:{}", params);
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
        String type = this.getCheckerType(incrementStrategy);
        MappingConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(mapping, params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(mapping, params);

        // 增量配置
        return JsonUtil.objToJson(mapping);
    }

}