package org.dbsyncer.storage.support;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.storage.AbstractStorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
@Component
@ConditionalOnProperty(value = "dbsyncer.storage.support.mysql", havingValue = "true")
@ConfigurationProperties(prefix = "dbsyncer.storage.support.mysql")
public class MysqlStorageServiceImpl extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private JdbcTemplate jdbcTemplate;

    private DatabaseConfig config;

    private Set<String> tables = new HashSet();

    private final Object createTableLock = new Object();

    @PostConstruct
    private void init() {
        config = null == config ? new DatabaseConfig() : config;
        config.setUrl(StringUtils.isNotBlank(config.getUrl()) ? config.getUrl()
                : "jdbc:mysql://127.0.0.1:3306/dbsyncer?rewriteBatchedStatements=true&seUnicode=true&characterEncoding=UTF8&useSSL=true");
        config.setDriverClassName(
                StringUtils.isNotBlank(config.getDriverClassName()) ? config.getDriverClassName() : "com.mysql.jdbc.Driver");
        config.setUsername(StringUtils.isNotBlank(config.getUsername()) ? config.getUsername() : "root");
        config.setPassword(StringUtils.isNotBlank(config.getPassword()) ? config.getPassword() : "123");
        logger.info("url:{}", config.getUrl());
        logger.info("driverClassName:{}", config.getDriverClassName());
        logger.info("username:{}", config.getUsername());
        logger.info("password:{}", config.getPassword());
        jdbcTemplate = DatabaseUtil.getJdbcTemplate(config);

        // 创建配置和日志表
        createTableIfNotExist(StorageEnum.CONFIG.getType());
        createTableIfNotExist(StorageEnum.LOG.getType());
    }

    @Override
    public List<Map> select(String table, Query query) {
        createTableIfNotExist(table);

        return null;
    }

    @Override
    public void insert(String table, Map params) {
        createTableIfNotExist(table);

    }

    @Override
    public void update(String table, Map params) {
        createTableIfNotExist(table);

    }

    @Override
    public void delete(String table, String id) {
        createTableIfNotExist(table);

    }

    @Override
    public void deleteAll(String table) {
        createTableIfNotExist(table);

    }

    @Override
    public void insertLog(String table, Map<String, Object> params) {
        createTableIfNotExist(table);

    }

    @Override
    public void insertData(String table, List<Map> list) {
        createTableIfNotExist(table);

    }

    @Override
    public void destroy() throws Exception {
        DatabaseUtil.close(jdbcTemplate);
    }

    private void createTableIfNotExist(String table) {
        synchronized (createTableLock) {
            // 1、检查本地缓存
            if (tables.contains(table)) {
                return;
            }

            // 2、检查DB中是否创建
            // show tables;
        }
    }

    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }
}