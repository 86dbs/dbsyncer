package org.dbsyncer.manager;

import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.query.Query;

import java.util.List;
import java.util.Map;

/**
 * 驱动配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 20:31
 */
public interface Manager extends Executor {

    /**
     * 添加ConfigModel
     *
     * @param model
     * @return id
     */
    String addConfigModel(ConfigModel model);

    /**
     * 编辑ConfigModel
     *
     * @param model
     * @return id
     */
    String editConfigModel(ConfigModel model);

    /**
     * 刪除ConfigModel
     *
      * @param id
     * @return
     */
    void removeConfigModel(String id);

    // system
    SystemConfig getSystemConfig();

    // user
    UserConfig getUserConfig();

    // project group
    ProjectGroup getProjectGroup(String id);

    List<ProjectGroup> getProjectGroupAll();

    // Connector
    ConnectorMapper connect(AbstractConnectorConfig config);

    boolean refreshConnectorConfig(AbstractConnectorConfig config);

    boolean isAliveConnectorConfig(AbstractConnectorConfig config);

    List<Table> getTable(ConnectorMapper config);

    MetaInfo getMetaInfo(String connectorId, String tableName);

    Connector getConnector(String connectorId);

    List<Connector> getConnectorAll();

    void checkAllConnectorStatus();

    // Mapping
    Mapping getMapping(String mappingId);

    List<Mapping> getMappingAll();

    // TableGroup
    String addTableGroup(TableGroup model);

    String editTableGroup(TableGroup model);

    TableGroup getTableGroup(String tableGroupId);

    List<TableGroup> getTableGroupAll(String mappingId);

    List<TableGroup> getSortedTableGroupAll(String mappingId);

    int getTableGroupCount(String mappingId);

    Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup);

    long getCount(String connectorId, Map<String, String> command);

    // Meta
    Meta getMeta(String metaId);

    List<Meta> getMetaAll();

    // Data
    Paging queryData(Query query);

    void removeData(String metaId, String messageId);

    void clearData(String metaId);

    // Log
    Paging queryLog(Query query);

    void clearLog();

    // ConnectorEnum
    List<ConnectorEnum> getConnectorEnumAll();

    // OperationEnum
    List<OperationEnum> getOperationEnumAll();

    // QuartzFilterEnum
    List<QuartzFilterEnum> getQuartzFilterEnumAll();

    // FilterEnum
    List<FilterEnum> getFilterEnumAll();

    // ConvertEnum
    List<ConvertEnum> getConvertEnumAll();

    // StorageDataStatusEnum
    List<StorageDataStatusEnum> getStorageDataStatusEnumAll();

    // Plugin
    List<Plugin> getPluginAll();

    String getPluginPath();

    String getLibraryPath();

    void loadPlugins();
}