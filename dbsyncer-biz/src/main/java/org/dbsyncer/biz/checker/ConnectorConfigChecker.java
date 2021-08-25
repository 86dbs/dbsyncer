package org.dbsyncer.biz.checker;

import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;

import java.util.Map;

/**
 * @param <C> ConnectorConfig
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface ConnectorConfigChecker<C> {

    /**
     * 处理增量同步策略
     *
     * @param mapping
     * @param tableGroup
     */
    default void dealIncrementStrategy(Mapping mapping, TableGroup tableGroup) {}

    /**
     * 修改配置
     *
     * @param connectorConfig
     * @param params
     * @return
     */
    void modify(C connectorConfig, Map<String, String> params);

}