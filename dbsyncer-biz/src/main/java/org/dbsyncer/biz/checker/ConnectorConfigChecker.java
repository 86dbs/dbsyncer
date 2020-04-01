package org.dbsyncer.biz.checker;

import org.dbsyncer.parser.model.Connector;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface ConnectorConfigChecker {

    /**
     * 修改配置
     *
     * @param connector
     * @param params
     * @return
     */
    void modify(Connector connector, Map<String, String> params);

}