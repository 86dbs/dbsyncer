package org.dbsyncer.manager;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.manager.template.*;
import org.dbsyncer.manager.template.impl.ConfigOperationTemplate;
import org.dbsyncer.manager.template.impl.ConfigPreLoadTemplate;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ManagerFactory implements Manager, ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private ConfigPreLoadTemplate preLoadTemplate;

    @Autowired
    private ConfigOperationTemplate operationTemplate;

    @Autowired
    private GroupStrategy defaultGroupStrategy;

    @Autowired
    private GroupStrategy tableGroupStrategy;

    @Override
    public boolean alive(ConnectorConfig config) {
        return parser.alive(config);
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return parser.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        return parser.getMetaInfo(connectorId, tableName);
    }

    @Override
    public String addConnector(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.add();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

        });
    }

    @Override
    public String editConnector(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.edit();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }
        });
    }

    @Override
    public void removeConnector(String connectorId) {
        operationTemplate.remove(new RemoveTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

            @Override
            public String getId() {
                return connectorId;
            }
        });
    }

    @Override
    public Connector getConnector(String connectorId) {
        return operationTemplate.queryObject(Connector.class, connectorId);
    }

    @Override
    public List<Connector> getConnectorAll() {
        return operationTemplate.queryAll(new QueryTemplate<Connector>() {

            @Override
            public ConfigModel getConfigModel() {
                Connector connector = new Connector();
                connector.setType(ConfigConstant.CONNECTOR);
                return connector;
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }
        });
    }

    @Override
    public String addMapping(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.add();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

        });
    }

    @Override
    public String editMapping(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.edit();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

        });
    }

    @Override
    public void removeMapping(String mappingId) {
        operationTemplate.remove(new RemoveTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

            @Override
            public String getId() {
                return mappingId;
            }
        });
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return operationTemplate.queryObject(Mapping.class, mappingId);
    }

    @Override
    public List<Mapping> getMappingAll() {
        return operationTemplate.queryAll(new QueryTemplate<Mapping>() {

            @Override
            public ConfigModel getConfigModel() {
                Mapping mapping = new Mapping();
                mapping.setType(ConfigConstant.MAPPING);
                return mapping;
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }
        });
    }

    @Override
    public String addTableGroup(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.add();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return tableGroupStrategy;
            }

        });
    }

    @Override
    public String editTableGroup(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.edit();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return tableGroupStrategy;
            }
        });
    }

    @Override
    public void removeTableGroup(String tableGroupId) {
        operationTemplate.remove(new RemoveTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return tableGroupStrategy;
            }

            @Override
            public String getId() {
                return tableGroupId;
            }
        });
    }

    @Override
    public TableGroup getTableGroup(String tableGroupId) {
        return operationTemplate.queryObject(TableGroup.class, tableGroupId);
    }

    @Override
    public List<TableGroup> getTableGroupAll(String mappingId) {
        return operationTemplate.queryAll(new QueryTemplate<TableGroup>() {
            @Override
            public ConfigModel getConfigModel() {
                TableGroup model = new TableGroup();
                model.setType(ConfigConstant.TABLE_GROUP);
                model.setMappingId(mappingId);
                return model;
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return tableGroupStrategy;
            }
        });
    }

    @Override
    public Map<String, String> getCommand(String sourceConnectorId, String targetConnectorId, TableGroup tableGroup) {
        return parser.getCommand(sourceConnectorId, targetConnectorId, tableGroup);
    }

    @Override
    public String addMeta(ConfigModel model) {
        return operationTemplate.execute(model, new OperationTemplate() {

            @Override
            public void handleEvent(ConfigOperationTemplate.Call call) {
                call.add();
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

        });
    }

    @Override
    public Meta getMeta(String metaId) {
        return operationTemplate.queryObject(Meta.class, metaId);
    }

    @Override
    public void removeMeta(String metaId) {
        operationTemplate.remove(new RemoveTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

            @Override
            public String getId() {
                return metaId;
            }
        });
    }

    @Override
    public List<Meta> getMetaAll() {
        return operationTemplate.queryAll(new QueryTemplate<Meta>() {

            @Override
            public ConfigModel getConfigModel() {
                Meta model = new Meta();
                model.setType(ConfigConstant.META);
                return model;
            }

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }
        });
    }

    @Override
    public List<ConnectorEnum> getConnectorEnumAll() {
        return parser.getConnectorEnumAll();
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return parser.getOperationEnumAll();
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return parser.getFilterEnumAll();
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return parser.getConvertEnumAll();
    }

    @Override
    public List<Plugin> getPluginAll() {
        return pluginFactory.getPluginAll();
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        // Load connectors
        preLoadTemplate.execute(new PreLoadTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

            @Override
            public String filterType() {
                return ConfigConstant.CONNECTOR;
            }

            @Override
            public ConfigModel parseModel(String json) {
                return parser.parseConnector(json);
            }
        });

        // Load mappings
        preLoadTemplate.execute(new PreLoadTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

            @Override
            public String filterType() {
                return ConfigConstant.MAPPING;
            }

            @Override
            public ConfigModel parseModel(String json) {
                return parser.parseObject(json, Mapping.class);
            }
        });

        // Load tableGroups
        preLoadTemplate.execute(new PreLoadTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return tableGroupStrategy;
            }

            @Override
            public String filterType() {
                return ConfigConstant.TABLE_GROUP;
            }

            @Override
            public ConfigModel parseModel(String json) {
                return parser.parseObject(json, TableGroup.class);
            }
        });

        // Load metas
        preLoadTemplate.execute(new PreLoadTemplate() {

            @Override
            public GroupStrategy getGroupStrategy() {
                return defaultGroupStrategy;
            }

            @Override
            public String filterType() {
                return ConfigConstant.META;
            }

            @Override
            public ConfigModel parseModel(String json) {
                return parser.parseObject(json, Meta.class);
            }
        });
    }

}