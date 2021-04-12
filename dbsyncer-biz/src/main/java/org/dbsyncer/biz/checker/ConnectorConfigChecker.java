package org.dbsyncer.biz.checker;

import org.dbsyncer.connector.config.Field;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface ConnectorConfigChecker {

    /**
     * 设置默认字段
     *
     * @param mapping
     * @param tableGroup
     * @param column
     * @param isSourceTable
     */
    default void updateFields(Mapping mapping, TableGroup tableGroup, List<Field> column, boolean isSourceTable) {}

    /**
     * 修改配置
     *
     * @param connector
     * @param params
     * @return
     */
    void modify(Connector connector, Map<String, String> params);

}