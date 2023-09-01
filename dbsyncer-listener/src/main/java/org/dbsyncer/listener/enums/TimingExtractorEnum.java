package org.dbsyncer.listener.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.quartz.DatabaseQuartzExtractor;
import org.dbsyncer.listener.quartz.ESQuartzExtractor;

/**
 * 定时模式支持类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum TimingExtractorEnum {

    /**
     * Mysql
     */
    MYSQL(ConnectorEnum.MYSQL.getType(), DatabaseQuartzExtractor.class),
    /**
     * Mysql
     */
    ORACLE(ConnectorEnum.ORACLE.getType(), DatabaseQuartzExtractor.class),
    /**
     * SqlServer
     */
    SQL_SERVER(ConnectorEnum.SQL_SERVER.getType(), DatabaseQuartzExtractor.class),
    /**
     * PostgreSQL
     */
    POSTGRE_SQL(ConnectorEnum.POSTGRE_SQL.getType(), DatabaseQuartzExtractor.class),
    /**
     * Elasticsearch
     */
    ELASTIC_SEARCH(ConnectorEnum.ELASTIC_SEARCH.getType(), ESQuartzExtractor.class),
    /**
     * DqlMysql
     */
    DQL_MYSQL(ConnectorEnum.DQL_MYSQL.getType(), DatabaseQuartzExtractor.class),
    /**
     * DqlOracle
     */
    DQL_ORACLE(ConnectorEnum.DQL_ORACLE.getType(), DatabaseQuartzExtractor.class),
    /**
     * DqlSqlServer
     */
    DQL_SQL_SERVER(ConnectorEnum.DQL_SQL_SERVER.getType(), DatabaseQuartzExtractor.class),
    /**
     * DqlPostgreSQL
     */
    DQL_POSTGRE_SQL(ConnectorEnum.DQL_POSTGRE_SQL.getType(), DatabaseQuartzExtractor.class);

    private String type;
    private Class clazz;

    TimingExtractorEnum(String type, Class clazz) {
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
    public static Class getExtractor(String type) throws ListenerException {
        for (TimingExtractorEnum e : TimingExtractorEnum.values()) {
            if (StringUtil.equals(type, e.getType())) {
                return e.getClazz();
            }
        }
        throw new ListenerException(String.format("TimingListenerEnum type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Class getClazz() {
        return clazz;
    }
}