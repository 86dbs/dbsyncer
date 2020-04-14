package org.dbsyncer.manager;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.config.Plugin;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 20:31
 */
public interface Manager {

    boolean alive(ConnectorConfig config);

    List<String> getTable(ConnectorConfig config);

    MetaInfo getMetaInfo(String connectorId, String tableName);

    // Connector
    String addConnector(ConfigModel model);

    String editConnector(ConfigModel model);

    void removeConnector(String connectorId);

    Connector getConnector(String connectorId);

    List<Connector> getConnectorAll();

    // Mapping
    String addMapping(ConfigModel model);

    String editMapping(ConfigModel model);

    void removeMapping(String mappingId);

    Mapping getMapping(String mappingId);

    List<Mapping> getMappingAll();

    // TableGroup
    String addTableGroup(ConfigModel model);

    String editTableGroup(ConfigModel model);

    void removeTableGroup(String tableGroupId);

    TableGroup getTableGroup(String tableGroupId);

    List<TableGroup> getTableGroupAll(String mappingId);

    Map<String, String> getCommand(String sourceConnectorId, String targetConnectorId, TableGroup tableGroup);

    // ConnectorEnum
    List<ConnectorEnum> getConnectorEnumAll();

    // OperationEnum
    List<OperationEnum> getOperationEnumAll();

    // FilterEnum
    List<FilterEnum> getFilterEnumAll();

    // ConvertEnum
    List<ConvertEnum> getConvertEnumAll();

    // Plugin
    List<Plugin> getPluginAll();

    /**
     * 启动驱动
     *
     * @param mappingId
     */
    void start(String mappingId);

    /**
     * 停止驱动
     *
     * @param mappingId
     */
    void stop(String mappingId);

}