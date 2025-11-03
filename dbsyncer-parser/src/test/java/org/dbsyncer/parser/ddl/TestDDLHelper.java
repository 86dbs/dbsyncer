package org.dbsyncer.parser.ddl;

import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.connector.sqlserver.SqlServerConnector;
import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.flush.impl.GeneralBufferActuator;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * DDL测试辅助类
 * 提供创建真实ConnectorService和配置TableGroup的辅助方法
 * 不使用mock，全部使用真实组件
 */
public class TestDDLHelper {

    private static final Logger logger = LoggerFactory.getLogger(TestDDLHelper.class);

    /**
     * 根据DatabaseConfig创建真实的ConnectorService
     *
     * @param config 数据库配置
     * @return ConnectorService实例
     */
    public static ConnectorService createConnectorService(DatabaseConfig config) {
        String connectorType = determineConnectorType(config);
        config.setConnectorType(connectorType);

        if ("MySQL".equals(connectorType)) {
            return new MySQLConnector();
        } else if ("SqlServer".equals(connectorType)) {
            return new SqlServerConnector();
        } else {
            // 默认使用MySQL
            logger.warn("无法确定连接器类型，默认使用MySQL。URL: {}", config.getUrl());
            return new MySQLConnector();
        }
    }

    /**
     * 创建并初始化ConnectorFactory
     * 使用反射调用init方法加载ServiceLoader中的连接器
     *
     * @return 初始化后的ConnectorFactory
     */
    public static ConnectorFactory createConnectorFactory() {
        ConnectorFactory factory = new ConnectorFactory();
        try {
            // 使用反射调用@PostConstruct方法
            Method initMethod = ConnectorFactory.class.getDeclaredMethod("init");
            initMethod.setAccessible(true);
            initMethod.invoke(factory);
            logger.info("ConnectorFactory初始化完成");
        } catch (Exception e) {
            logger.error("初始化ConnectorFactory失败", e);
            throw new RuntimeException("无法初始化ConnectorFactory", e);
        }
        return factory;
    }

    /**
     * 初始化DDLParserImpl
     * 调用@PostConstruct的init方法初始化STRATEGIES
     *
     * @param ddlParser DDL解析器实例
     */
    public static void initDDLParser(DDLParserImpl ddlParser) {
        try {
            Method initMethod = DDLParserImpl.class.getDeclaredMethod("init");
            initMethod.setAccessible(true);
            initMethod.invoke(ddlParser);
            logger.info("DDLParserImpl初始化完成");
        } catch (Exception e) {
            logger.error("初始化DDLParserImpl失败", e);
            throw new RuntimeException("无法初始化DDLParserImpl", e);
        }
    }

    /**
     * 设置ConnectorFactory到DDLParserImpl
     *
     * @param ddlParser DDL解析器实例
     * @param connectorFactory ConnectorFactory实例
     */
    public static void setConnectorFactory(DDLParserImpl ddlParser, ConnectorFactory connectorFactory) {
        try {
            Field field = DDLParserImpl.class.getDeclaredField("connectorFactory");
            field.setAccessible(true);
            field.set(ddlParser, connectorFactory);
            logger.info("已设置ConnectorFactory到DDLParserImpl");
        } catch (Exception e) {
            logger.error("设置ConnectorFactory失败", e);
            throw new RuntimeException("无法设置ConnectorFactory", e);
        }
    }

    /**
     * 创建测试用的ProfileComponent实现
     */
    public static ProfileComponent createTestProfileComponent(String mappingId,
                                                               String sourceConnectorId, DatabaseConfig sourceConfig,
                                                               String targetConnectorId, DatabaseConfig targetConfig) {
        return new TestProfileComponent(mappingId, sourceConnectorId, sourceConfig, targetConnectorId, targetConfig);
    }

    /**
     * 配置TableGroup，包括profileComponent和Mapping信息
     *
     * @param tableGroup         TableGroup实例
     * @param mappingId          Mapping ID
     * @param sourceConnectorId  源连接器ID
     * @param targetConnectorId  目标连接器ID
     * @param sourceConfig       源数据库配置
     * @param targetConfig       目标数据库配置
     */
    public static void setupTableGroup(TableGroup tableGroup, String mappingId,
                                       String sourceConnectorId, String targetConnectorId,
                                       DatabaseConfig sourceConfig, DatabaseConfig targetConfig) {
        // 设置connectorType
        sourceConfig.setConnectorType(determineConnectorType(sourceConfig));
        targetConfig.setConnectorType(determineConnectorType(targetConfig));

        // 创建测试用的ProfileComponent
        ProfileComponent profileComponent = createTestProfileComponent(
                mappingId, sourceConnectorId, sourceConfig, targetConnectorId, targetConfig);

        // 设置TableGroup的profileComponent
        tableGroup.profileComponent = profileComponent;
        tableGroup.setMappingId(mappingId);
    }

    /**
     * 创建测试用的WriterResponse
     *
     * @param sql DDL SQL语句
     * @param event 事件类型（如"ALTER"）
     * @param tableName 表名
     * @return WriterResponse实例
     */
    public static WriterResponse createWriterResponse(String sql, String event, String tableName) {
        WriterResponse response = new WriterResponse();
        response.setSql(sql);
        response.setEvent(event);
        response.setTableName(tableName);
        response.setTypeEnum(ChangedEventTypeEnum.DDL);

        // 创建ChangedOffset
        ChangedOffset changedOffset = new ChangedOffset();
        changedOffset.setMetaId("test-meta-id");
        response.setChangedOffset(changedOffset);
        
        return response;
    }

    /**
     * 创建测试用的Mapping
     *
     * @param mappingId Mapping ID
     * @param sourceConnectorId 源连接器ID
     * @param targetConnectorId 目标连接器ID
     * @param enableDDL 是否启用DDL同步
     * @param metaId Meta ID
     * @return Mapping实例
     */
    public static Mapping createMapping(String mappingId,
                                        String sourceConnectorId,
                                        String targetConnectorId,
                                        boolean enableDDL,
                                        String metaId) {
        Mapping mapping = new Mapping();
        mapping.setId(mappingId);
        mapping.setSourceConnectorId(sourceConnectorId);
        mapping.setTargetConnectorId(targetConnectorId);
        mapping.setMetaId(metaId);

        // 创建ListenerConfig
        ListenerConfig listenerConfig = new ListenerConfig();
        listenerConfig.setEnableDDL(enableDDL);
        mapping.setListener(listenerConfig);
        
        return mapping;
    }

    /**
     * 创建并配置GeneralBufferActuator用于测试
     * 使用反射设置必要的依赖，对于测试中不需要的依赖使用空实现
     *
     * @param connectorFactory ConnectorFactory实例
     * @param profileComponent ProfileComponent实例（从TableGroup中获取）
     * @param ddlParser DDLParser实例
     * @return 配置好的GeneralBufferActuator实例
     */
    public static GeneralBufferActuator createGeneralBufferActuator(
            ConnectorFactory connectorFactory,
            ProfileComponent profileComponent,
            DDLParserImpl ddlParser) {
        GeneralBufferActuator actuator = new GeneralBufferActuator();

        try {
            // 使用反射设置依赖
            setField(actuator, "connectorFactory", connectorFactory);
            setField(actuator, "profileComponent", profileComponent);
            setField(actuator, "ddlParser", ddlParser);

            // 创建空的FlushStrategy实现（测试中不需要实际持久化）
            FlushStrategy flushStrategy = new FlushStrategy() {
                @Override
                public void flushFullData(String metaId, Result result, String event) {
                    // 测试中不需要实际持久化
                }

                @Override
                public void flushIncrementData(String metaId, Result result, String event) {
                    // 测试中不需要实际持久化
                }
            };
            setField(actuator, "flushStrategy", flushStrategy);

            // BufferActuatorRouter是final类，直接创建实例即可（测试中refreshOffset方法不会真正执行刷新逻辑，因为meta为null）
            BufferActuatorRouter bufferActuatorRouter = new BufferActuatorRouter();
            // 使用反射设置profileComponent（refreshOffset需要用到，但如果meta为null会直接返回，不会报错）
            setField(bufferActuatorRouter, "profileComponent", profileComponent);
            setField(actuator, "bufferActuatorRouter", bufferActuatorRouter);

            // PluginFactory有@PostConstruct方法，需要避免初始化，使用null即可（parseDDl不会调用pluginFactory）
            // 如果需要的话，可以创建一个空的实现，但测试中parseDDl不会用到pluginFactory
            setField(actuator, "pluginFactory", null);

            logger.info("GeneralBufferActuator配置完成");
        } catch (Exception e) {
            logger.error("配置GeneralBufferActuator失败", e);
            throw new RuntimeException("无法配置GeneralBufferActuator", e);
        }

        return actuator;
    }

    /**
     * 使用反射设置字段值
     */
    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    /**
     * 从URL推断连接器类型
     *
     * @param config 数据库配置
     * @return 连接器类型
     */
    private static String determineConnectorType(DatabaseConfig config) {
        if (config.getConnectorType() != null && !config.getConnectorType().isEmpty()) {
            return config.getConnectorType();
        }

        String url = config.getUrl();
        if (url == null) {
            return "MySQL"; // 默认
        }

        String urlLower = url.toLowerCase();
        if (urlLower.contains("mysql")) {
            return "MySQL";
        } else if (urlLower.contains("sqlserver") || urlLower.contains("jdbc:sqlserver")) {
            return "SqlServer";
        }
        return "MySQL"; // 默认
    }

    /**
     * 测试用的ProfileComponent实现
     * 提供测试所需的最小实现
     */
    private static class TestProfileComponent implements ProfileComponent {
        private final Map<String, Mapping> mappings = new HashMap<>();
        private final Map<String, Connector> connectors = new HashMap<>();

        public TestProfileComponent(String mappingId,
                                   String sourceConnectorId, DatabaseConfig sourceConfig,
                                   String targetConnectorId, DatabaseConfig targetConfig) {
            // 创建Mapping
            Mapping mapping = new Mapping();
            mapping.setId(mappingId);
            mapping.setSourceConnectorId(sourceConnectorId);
            mapping.setTargetConnectorId(targetConnectorId);
            mappings.put(mappingId, mapping);

            // 创建源Connector
            Connector sourceConnector = new Connector();
            sourceConnector.setId(sourceConnectorId);
            sourceConnector.setConfig(sourceConfig);
            connectors.put(sourceConnectorId, sourceConnector);

            // 创建目标Connector
            Connector targetConnector = new Connector();
            targetConnector.setId(targetConnectorId);
            targetConnector.setConfig(targetConfig);
            connectors.put(targetConnectorId, targetConnector);
        }

        @Override
        public Mapping getMapping(String mappingId) {
            return mappings.get(mappingId);
        }

        @Override
        public Connector getConnector(String connectorId) {
            return connectors.get(connectorId);
        }

        // 其他方法返回null或空集合，测试中不需要
        @Override
        public org.dbsyncer.parser.model.Connector parseConnector(String json) {
            return null;
        }

        @Override
        public <T> T parseObject(String json, Class<T> clazz) {
            return null;
        }

        @Override
        public String addConfigModel(org.dbsyncer.parser.model.ConfigModel model) {
            return null;
        }

        @Override
        public String editConfigModel(org.dbsyncer.parser.model.ConfigModel model) {
            return null;
        }

        @Override
        public void removeConfigModel(String id) {
        }

        @Override
        public org.dbsyncer.parser.model.SystemConfig getSystemConfig() {
            return null;
        }

        @Override
        public org.dbsyncer.parser.model.UserConfig getUserConfig() {
            return null;
        }

        @Override
        public org.dbsyncer.parser.model.ProjectGroup getProjectGroup(String id) {
            return null;
        }

        @Override
        public java.util.List<org.dbsyncer.parser.model.ProjectGroup> getProjectGroupAll() {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<Connector> getConnectorAll() {
            return new java.util.ArrayList<>(connectors.values());
        }

        @Override
        public java.util.List<Mapping> getMappingAll() {
            return new java.util.ArrayList<>(mappings.values());
        }

        @Override
        public String addTableGroup(org.dbsyncer.parser.model.TableGroup model) {
            return null;
        }

        @Override
        public String editTableGroup(org.dbsyncer.parser.model.TableGroup model) {
            return null;
        }

        @Override
        public void removeTableGroup(String id) {
        }

        @Override
        public org.dbsyncer.parser.model.TableGroup getTableGroup(String tableGroupId) {
            return null;
        }

        @Override
        public java.util.List<org.dbsyncer.parser.model.TableGroup> getTableGroupAll(String mappingId) {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<org.dbsyncer.parser.model.TableGroup> getSortedTableGroupAll(String mappingId) {
            return new java.util.ArrayList<>();
        }

        @Override
        public int getTableGroupCount(String mappingId) {
            return 0;
        }

        @Override
        public org.dbsyncer.parser.model.Meta getMeta(String metaId) {
            return null;
        }

        @Override
        public java.util.List<org.dbsyncer.parser.model.Meta> getMetaAll() {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<org.dbsyncer.sdk.enums.OperationEnum> getOperationEnumAll() {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<org.dbsyncer.sdk.enums.QuartzFilterEnum> getQuartzFilterEnumAll() {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<org.dbsyncer.sdk.enums.FilterEnum> getFilterEnumAll() {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<org.dbsyncer.parser.enums.ConvertEnum> getConvertEnumAll() {
            return new java.util.ArrayList<>();
        }

        @Override
        public java.util.List<org.dbsyncer.storage.enums.StorageDataStatusEnum> getStorageDataStatusEnumAll() {
            return new java.util.ArrayList<>();
        }
    }
}
