/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
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
        // 驱动名称
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        if(StringUtils.isNotBlank(name)){
            mapping.setName(name);
        }
        mapping.setUpdateTime(System.currentTimeMillis());
    }

}