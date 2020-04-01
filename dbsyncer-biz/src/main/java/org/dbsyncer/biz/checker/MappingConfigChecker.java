package org.dbsyncer.biz.checker;

import org.dbsyncer.parser.model.Mapping;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface MappingConfigChecker {

    /**
     * 修改配置
     *
     * @param mapping
     * @param params
     * @return
     */
    void modify(Mapping mapping, Map<String, String> params);

}