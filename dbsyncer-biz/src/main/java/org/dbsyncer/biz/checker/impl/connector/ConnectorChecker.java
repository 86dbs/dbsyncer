/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
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

        // 连接并获取数据库列表
        ConnectorInstance connectorInstance = connectorFactory.connect(config);
        List<String> databaseNames = fetchDatabaseNames(connectorInstance, connectorType);
        connector.setDataBaseName(databaseNames);

        // 获取Schema列表（仅Oracle直接获取）
        List<String> schemaNames = fetchSchemaNames(connectorInstance, connectorType, null);
        connector.setSchemaName(schemaNames);

        // 修改基本配置
        this.modifyConfigModel(connector, params);

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
        connectorFactory.disconnect(config);

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 连接器配置校验
        ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
        ConfigValidator configValidator = connectorService.getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        configValidator.modify(connectorService, config, params);

        // 连接并获取数据库列表
        ConnectorInstance connectorInstance = connectorFactory.connect(config);
        String connectorType = config.getConnectorType();
        List<String> databaseNames = fetchDatabaseNames(connectorInstance, connectorType);
        connector.setDataBaseName(databaseNames);

        // 获取Schema列表（仅Oracle直接获取）
        List<String> schemaNames = fetchSchemaNames(connectorInstance, connectorType, null);
        connector.setSchemaName(schemaNames);

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

    /**
     * 获取数据库名称列表
     * 
     * @param connectorInstance 连接器实例
     * @param connectorType 连接器类型
     * @return 数据库名称列表
     */
    private List<String> fetchDatabaseNames(ConnectorInstance connectorInstance, String connectorType) {
        List<String> databases = new ArrayList<>();
        
        try {
            // 只处理数据库类型的连接器
            if (!(connectorInstance instanceof DatabaseConnectorInstance)) {
                logger.info("连接器类型{}不支持获取数据库列表", connectorType);
                return databases;
            }
            
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
            
        } catch (Exception e) {
            logger.error("获取数据库列表失败: {}", e.getMessage(), e);
            // 不抛出异常，返回空列表
        }
        
        return databases;
    }

    /**
     * 获取Schema名称列表
     * 
     * @param connectorInstance 连接器实例
     * @param connectorType 连接器类型
     * @param databaseName 数据库名称（PostgreSQL/SQL Server需要，Oracle不需要）
     * @return Schema名称列表
     */
    private List<String> fetchSchemaNames(ConnectorInstance connectorInstance, String connectorType, String databaseName) {
        List<String> schemas = new ArrayList<>();
        
        try {
            // 只处理数据库类型的连接器
            if (!(connectorInstance instanceof DatabaseConnectorInstance)) {
                logger.info("连接器类型{}不支持获取Schema列表", connectorType);
                return schemas;
            }
            
            DatabaseConnectorInstance connection = (DatabaseConnectorInstance) connectorInstance;
            String type = connectorType.toLowerCase();
            
            if (type.contains("oracle")) {
                // Oracle: 直接获取所有用户Schema
                schemas = connection.execute(databaseTemplate -> {
                    List<String> schemaList = new ArrayList<>();
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement()
                            .executeQuery("SELECT username FROM all_users ORDER BY username")) {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            if (!isSystemSchema(schemaName, "oracle")) {
                                schemaList.add(schemaName);
                            }
                        }
                    }
                    return schemaList;
                });
                logger.info("成功获取Oracle Schema列表，共{}个Schema", schemas.size());
                
            } else if (type.contains("postgresql")) {
                // PostgreSQL: 需要指定数据库名称
                if (databaseName == null || databaseName.isEmpty()) {
                    logger.info("PostgreSQL获取Schema需要指定数据库名称");
                    return schemas;
                }
                
                // 切换到指定数据库获取Schema
                schemas = fetchPostgreSQLSchemas(connection, databaseName);
                logger.info("成功获取PostgreSQL数据库[{}]的Schema列表，共{}个Schema", databaseName, schemas.size());
                
            } else if (type.contains("sqlserver")) {
                // SQL Server: 需要指定数据库名称
                if (databaseName == null || databaseName.isEmpty()) {
                    logger.info("SQL Server获取Schema需要指定数据库名称");
                    return schemas;
                }
                
                // 切换到指定数据库获取Schema
                schemas = fetchSQLServerSchemas(connection, databaseName);
                logger.info("成功获取SQL Server数据库[{}]的Schema列表，共{}个Schema", databaseName, schemas.size());
                
            } else {
                logger.info("连接器类型{}暂不支持获取Schema列表", connectorType);
            }
            
        } catch (Exception e) {
            logger.error("获取Schema列表失败: {}", e.getMessage(), e);
        }
        
        return schemas;
    }

    /**
     * 获取PostgreSQL指定数据库的Schema列表
     */
    private List<String> fetchPostgreSQLSchemas(DatabaseConnectorInstance connection, String databaseName) {
        return connection.execute(databaseTemplate -> {
            List<String> schemaList = new ArrayList<>();
            String sql = String.format(
                "SELECT schema_name FROM information_schema.schemata " +
                "WHERE catalog_name = '%s' AND schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')",
                databaseName
            );
            try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery(sql)) {
                while (rs.next()) {
                    String schemaName = rs.getString(1);
                    schemaList.add(schemaName);
                }
            }
            return schemaList;
        });
    }

    /**
     * 获取SQL Server指定数据库的Schema列表
     */
    private List<String> fetchSQLServerSchemas(DatabaseConnectorInstance connection, String databaseName) {
        return connection.execute(databaseTemplate -> {
            List<String> schemaList = new ArrayList<>();
            String sql = String.format(
                "SELECT name FROM [%s].sys.schemas " +
                "WHERE name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', " +
                "'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')",
                databaseName
            );
            try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery(sql)) {
                while (rs.next()) {
                    String schemaName = rs.getString(1);
                    schemaList.add(schemaName);
                }
            }
            return schemaList;
        });
    }

    /**
     * 判断是否为系统Schema
     * 
     * @param schemaName Schema名称
     * @param dbType 数据库类型
     * @return true表示系统Schema
     */
    private boolean isSystemSchema(String schemaName, String dbType) {
        if (schemaName == null || schemaName.isEmpty()) {
            return true;
        }
        
        Set<String> systemSchemas = new HashSet<>();
        
        if ("oracle".equalsIgnoreCase(dbType)) {
            // Oracle系统用户
            systemSchemas.add("SYS");
            systemSchemas.add("SYSTEM");
            systemSchemas.add("DBSNMP");
            systemSchemas.add("SYSMAN");
            systemSchemas.add("OUTLN");
            systemSchemas.add("MDSYS");
            systemSchemas.add("ORDSYS");
            systemSchemas.add("EXFSYS");
            systemSchemas.add("CTXSYS");
            systemSchemas.add("XDB");
            systemSchemas.add("ANONYMOUS");
            systemSchemas.add("ORACLE_OCM");
            systemSchemas.add("APPQOSSYS");
            systemSchemas.add("WMSYS");
            systemSchemas.add("APEX_030200");
            systemSchemas.add("APEX_PUBLIC_USER");
            systemSchemas.add("FLOWS_FILES");
        }
        
        return systemSchemas.contains(schemaName.toUpperCase());
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