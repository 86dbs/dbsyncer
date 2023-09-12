package org.dbsyncer.connector.enums;

import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ESConfig;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.es.ESConnector;
import org.dbsyncer.connector.file.FileConnector;
import org.dbsyncer.connector.kafka.KafkaConnector;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.connector.oracle.OracleConnector;
import org.dbsyncer.connector.postgresql.PostgreSQLConnector;
import org.dbsyncer.connector.sql.DQLMySQLConnector;
import org.dbsyncer.connector.sql.DQLOracleConnector;
import org.dbsyncer.connector.sql.DQLPostgreSQLConnector;
import org.dbsyncer.connector.sql.DQLSqlServerConnector;
import org.dbsyncer.connector.sqlserver.SqlServerConnector;

/**
 * 支持的连接器类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:56
 */
public enum ConnectorEnum {

    /**
     * MySQL 连接器
     */
    MYSQL("MySQL", new MySQLConnector(), DatabaseConfig.class),
    /**
     * Oracle 连接器
     */
    ORACLE("Oracle", new OracleConnector(), DatabaseConfig.class),
    /**
     * SqlServer 连接器
     */
    SQL_SERVER("SqlServer", new SqlServerConnector(), DatabaseConfig.class),
    /**
     * PostgreSQL 连接器
     */
    POSTGRE_SQL("PostgreSQL", new PostgreSQLConnector(), DatabaseConfig.class),
    /**
     * Elasticsearch 连接器
     */
    ELASTIC_SEARCH("Elasticsearch", new ESConnector(), ESConfig.class),
    /**
     * Kafka 连接器
     */
    KAFKA("Kafka", new KafkaConnector(), KafkaConfig.class),
    /**
     * File 连接器
     */
    FILE("File", new FileConnector(), FileConfig.class),
    /**
     * DqlMySQL 连接器
     */
    DQL_MYSQL("DqlMySQL", new DQLMySQLConnector(), DatabaseConfig.class),
    /**
     * DqlOracle 连接器
     */
    DQL_ORACLE("DqlOracle", new DQLOracleConnector(), DatabaseConfig.class),
    /**
     * DqlSqlServer 连接器
     */
    DQL_SQL_SERVER("DqlSqlServer", new DQLSqlServerConnector(), DatabaseConfig.class),
    /**
     * DqlPostgreSQL 连接器
     */
    DQL_POSTGRE_SQL("DqlPostgreSQL", new DQLPostgreSQLConnector(), DatabaseConfig.class);

    // 连接器名称
    private String type;

    // 连接器
    private Connector connector;

    // 配置
    private Class<? extends AbstractConnectorConfig> configClass;

    ConnectorEnum(String type, Connector connector, Class<? extends AbstractConnectorConfig> configClass) {
        this.type = type;
        this.connector = connector;
        this.configClass = configClass;
    }

    /**
     * 获取连接枚举
     *
     * @param type
     * @return
     * @throws ConnectorException
     */
    public static ConnectorEnum getConnectorEnum(String type) throws ConnectorException {
        for (ConnectorEnum e : ConnectorEnum.values()) {
            if (StringUtil.equalsIgnoreCase(type, e.getType())) {
                return e;
            }
        }
        throw new ConnectorException(String.format("Connector type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Connector getConnector() {
        return connector;
    }

    public Class<? extends AbstractConnectorConfig> getConfigClass() {
        return configClass;
    }
}