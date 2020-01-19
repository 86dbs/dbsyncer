package org.dbsyncer.biz.checker;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface Checker {

    /**
     * 修改连接器配置
     *
     * @param connector
     * @param params
     */
    void modify(Connector connector, Map<String, String> params);

    /**
     * 修改驱动配置
     *
     * @param connector
     * @param params
     */
    void modify(Mapping connector, Map<String, String> params);

}