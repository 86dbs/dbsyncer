/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
@Component
public class ConnectorChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        printParams(params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String connectorType = params.get("connectorType");
        Assert.hasText(name, "connector name is empty.");
        Assert.hasText(connectorType, "connector connectorType is empty.");

        Connector connector = new Connector();
        connector.setName(name);
        ConnectorConfig config = getConfig(connectorType);
        connector.setConfig(config);

        // 连接器配置校验
        ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
        ConfigValidator configValidator = connectorService.getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        configValidator.modify(connectorService, config, params);

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 连接并获取数据库列表
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), config, StringUtil.EMPTY, StringUtil.EMPTY);
        connector.setDatabase(queryDatabase(connectorInstance, connectorType));
        // TODO
        connector.setSchema(null);

        return connector;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "ConnectorChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");
        ConnectorConfig config = connector.getConfig();
        connectorFactory.disconnect(connector.getId());

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 连接器配置校验
        ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
        ConfigValidator configValidator = connectorService.getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        configValidator.modify(connectorService, config, params);

        // 连接并获取数据库列表
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), config, StringUtil.EMPTY, StringUtil.EMPTY);
        String connectorType = config.getConnectorType();
        connector.setDatabase(queryDatabase(connectorInstance, connectorType));
        // TODO
        connector.setSchema(null);

        return connector;
    }

    private ConnectorConfig getConfig(String connectorType) {
        try {
            ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
            Class<ConnectorConfig> configClass = connectorService.getConfigClass();
            ConnectorConfig config = configClass.newInstance();
            config.setConnectorType(connectorService.getConnectorType());
            return config;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new BizException("获取连接器配置异常.");
        }
    }

    private List<String> queryDatabase(ConnectorInstance connectorInstance, String connectorType) {
        List<String> databases = new ArrayList<>();
        // 只处理数据库类型的连接器
        if (!(connectorInstance instanceof DatabaseConnectorInstance)) {
            return databases;
        }

        // TODO 应抽象连接器实现
        DatabaseConnectorInstance connection = (DatabaseConnectorInstance) connectorInstance;
        String type = connectorType.toLowerCase();
        if (type.contains("mysql")) {
            // MySQL: SHOW DATABASES
            databases = connection.execute(databaseTemplate -> {
                List<String> dbList = new ArrayList<>();
                try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement()
                        .executeQuery("SHOW DATABASES")) {
                    while (rs.next()) {
                        String dbName = rs.getString(1);
                        // 过滤系统数据库
                        if (!isSystemDatabase(dbName)) {
                            dbList.add(dbName);
                        }
                    }
                }
                return dbList;
            });
            logger.info("成功获取MySQL数据库列表，共{}个数据库", databases.size());

        } else if (type.contains("postgresql")) {
            // PostgreSQL: SELECT datname FROM pg_database
            databases = connection.execute(databaseTemplate -> {
                List<String> dbList = new ArrayList<>();
                try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement()
                        .executeQuery("SELECT datname FROM pg_database WHERE datistemplate = false")) {
                    while (rs.next()) {
                        String dbName = rs.getString(1);
                        if (!isSystemDatabase(dbName)) {
                            dbList.add(dbName);
                        }
                    }
                }
                return dbList;
            });
            logger.info("成功获取PostgreSQL数据库列表，共{}个数据库", databases.size());

        } else if (type.contains("oracle")) {
            // Oracle: SELECT username FROM all_users
            databases = connection.execute(databaseTemplate -> {
                List<String> dbList = new ArrayList<>();
                try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement()
                        .executeQuery("SELECT username FROM all_users ORDER BY username")) {
                    while (rs.next()) {
                        String dbName = rs.getString(1);
                        if (!isSystemDatabase(dbName)) {
                            dbList.add(dbName);
                        }
                    }
                }
                return dbList;
            });
            logger.info("成功获取Oracle用户列表，共{}个用户", databases.size());

        } else if (type.contains("sqlserver")) {
            // SQL Server: SELECT name FROM sys.databases
            databases = connection.execute(databaseTemplate -> {
                List<String> dbList = new ArrayList<>();
                try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement()
                        .executeQuery("SELECT name FROM sys.databases WHERE database_id > 4")) {
                    while (rs.next()) {
                        String dbName = rs.getString(1);
                        if (!isSystemDatabase(dbName)) {
                            dbList.add(dbName);
                        }
                    }
                }
                return dbList;
            });
            logger.info("成功获取SQL Server数据库列表，共{}个数据库", databases.size());

        } else {
            // 其他数据库类型暂不支持
            logger.info("连接器类型{}暂不支持自动获取数据库列表", connectorType);
        }
        return databases;
    }

    /**
     * 判断是否为系统数据库
     * 
     * @param dbName 数据库名称
     * @return true表示系统数据库
     */
    private boolean isSystemDatabase(String dbName) {
        if (dbName == null || dbName.isEmpty()) {
            return true;
        }
        
        // 定义系统数据库列表
        Set<String> systemDatabases = new HashSet<>();
        
        // MySQL系统数据库
        systemDatabases.add("information_schema");
        systemDatabases.add("mysql");
        systemDatabases.add("performance_schema");
        systemDatabases.add("sys");
        
        // PostgreSQL系统数据库
        systemDatabases.add("postgres");
        systemDatabases.add("template0");
        systemDatabases.add("template1");
        
        // Oracle系统用户
        systemDatabases.add("SYS");
        systemDatabases.add("SYSTEM");
        systemDatabases.add("DBSNMP");
        systemDatabases.add("SYSMAN");
        systemDatabases.add("OUTLN");
        systemDatabases.add("MDSYS");
        systemDatabases.add("ORDSYS");
        systemDatabases.add("EXFSYS");
        systemDatabases.add("CTXSYS");
        systemDatabases.add("XDB");
        systemDatabases.add("ANONYMOUS");
        systemDatabases.add("ORACLE_OCM");
        systemDatabases.add("APPQOSSYS");
        systemDatabases.add("WMSYS");
        
        // SQL Server系统数据库已通过SQL过滤 (database_id > 4)
        systemDatabases.add("master");
        systemDatabases.add("tempdb");
        systemDatabases.add("model");
        systemDatabases.add("msdb");
        
        return systemDatabases.contains(dbName.toLowerCase()) || 
               systemDatabases.contains(dbName.toUpperCase());
    }

}