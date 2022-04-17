package org.dbsyncer.listener.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.kafka.KafkaExtractor;
import org.dbsyncer.listener.mysql.MysqlExtractor;
import org.dbsyncer.listener.oracle.OracleExtractor;
import org.dbsyncer.listener.postgresql.PostgreSQLExtractor;
import org.dbsyncer.listener.quartz.DatabaseQuartzExtractor;
import org.dbsyncer.listener.quartz.ESQuartzExtractor;
import org.dbsyncer.listener.sqlserver.SqlServerExtractor;

/**
 * 监听器Extractor支持日志和定时模式
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum ListenerEnum {

    /**
     * log_Mysql
     */
    LOG_MYSQL(ListenerTypeEnum.LOG.getType() + ConnectorEnum.MYSQL.getType(), MysqlExtractor.class),
    /**
     * log_Oracle
     */
    LOG_ORACLE(ListenerTypeEnum.LOG.getType() + ConnectorEnum.ORACLE.getType(), OracleExtractor.class),
    /**
     * log_SqlServer
     */
    LOG_SQL_SERVER(ListenerTypeEnum.LOG.getType() + ConnectorEnum.SQL_SERVER.getType(), SqlServerExtractor.class),
    /**
     * log_PostgreSQL
     */
    LOG_POSTGRE_SQL(ListenerTypeEnum.LOG.getType() + ConnectorEnum.POSTGRE_SQL.getType(), PostgreSQLExtractor.class),
    /**
     * log_Kafka
     */
    LOG_KAFKA(ListenerTypeEnum.LOG.getType() + ConnectorEnum.KAFKA.getType(), KafkaExtractor.class),
    /**
     * timing_Mysql
     */
    TIMING_MYSQL(ListenerTypeEnum.TIMING.getType() + ConnectorEnum.MYSQL.getType(), DatabaseQuartzExtractor.class),
    /**
     * timing_Mysql
     */
    TIMING_ORACLE(ListenerTypeEnum.TIMING.getType() + ConnectorEnum.ORACLE.getType(), DatabaseQuartzExtractor.class),
    /**
     * timing_SqlServer
     */
    TIMING_SQL_SERVER(ListenerTypeEnum.TIMING.getType() + ConnectorEnum.SQL_SERVER.getType(), DatabaseQuartzExtractor.class),
    /**
     * timing_PostgreSQL
     */
    TIMING_POSTGRE_SQL(ListenerTypeEnum.TIMING.getType() + ConnectorEnum.POSTGRE_SQL.getType(), DatabaseQuartzExtractor.class),
    /**
     * timing_Elasticsearch
     */
    TIMING_ELASTIC_SEARCH(ListenerTypeEnum.TIMING.getType() + ConnectorEnum.ELASTIC_SEARCH.getType(), ESQuartzExtractor.class);

    private String type;
    private Class<?> clazz;

    ListenerEnum(String type, Class<?> clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    /**
     * 获取抽取器
     *
     * @param type
     * @return
     * @throws ListenerException
     */
    public static Class<?> getExtractor(String type) throws ListenerException {
        for (ListenerEnum e : ListenerEnum.values()) {
            if (StringUtil.equals(type, e.getType())) {
                return e.getClazz();
            }
        }
        throw new ListenerException(String.format("Extractor type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}