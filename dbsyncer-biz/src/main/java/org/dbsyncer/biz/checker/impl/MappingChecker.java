/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.parser.constant.ModelConstant;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public class MappingChecker extends AbstractChecker {

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        // 名称
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        if(StringUtils.isNotBlank(name)){
            mapping.setName(name);
        }

        // 同步方式(仅支持全量或增量同步方式)
        String model = params.get("model");
        if(StringUtils.isNotBlank(model)){
            if(StringUtils.equals(ModelConstant.FULL, model) || StringUtils.equals(ModelConstant.INCREMENT, model)){
                mapping.setModel(model);
            }
        }

        // 全量配置
        String threadNum = params.get("threadNum");
        mapping.setThreadNum(NumberUtils.toInt(threadNum, mapping.getThreadNum()));
        String batchNum = params.get("batchNum");
        mapping.setBatchNum(NumberUtils.toInt(batchNum, mapping.getBatchNum()));
        // TODO 增量配置

        // 修改：过滤条件/转换配置/插件配置
        modifyConfigModel(mapping, params);

        // 增量配置
        mapping.setUpdateTime(System.currentTimeMillis());
    }

}