/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public class MappingChecker extends AbstractChecker {

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        String name = params.get("name");
        Assert.hasText(name, "MappingChecker modify name is empty.");

        mapping.setName(name);
        mapping.setUpdateTime(System.currentTimeMillis());
    }

}