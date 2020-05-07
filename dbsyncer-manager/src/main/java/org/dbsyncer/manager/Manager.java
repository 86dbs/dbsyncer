package org.dbsyncer.manager;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.plugin.config.Plugin;

import java.util.List;
import java.util.Map;

/**
 * 驱动配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 20:31
 */
public interface Manager extends Task {

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

    Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup);

    long getCount(String connectorId, Map<String, String> command);

    // Meta
    String addMeta(ConfigModel model);

    String editMeta(ConfigModel model);

    Meta getMeta(String metaId);

    void removeMeta(String metaId);

    List<Meta> getMetaAll();

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

}