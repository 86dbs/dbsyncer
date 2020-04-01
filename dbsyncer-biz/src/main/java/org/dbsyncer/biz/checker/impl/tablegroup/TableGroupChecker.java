/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.tablegroup;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.constant.ModelConstant;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class TableGroupChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Override
    public String checkConfigModel(Map<String, String> params) {
        logger.info("check tableGroup params:{}", params);
        Assert.notEmpty(params, "TableGroupChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = manager.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(tableGroup, params);

        return JsonUtil.objToJson(tableGroup);
    }
}