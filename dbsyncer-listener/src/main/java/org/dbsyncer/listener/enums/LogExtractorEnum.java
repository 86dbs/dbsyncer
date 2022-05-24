package org.dbsyncer.listener.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.file.FileExtractor;
import org.dbsyncer.listener.kafka.KafkaExtractor;
import org.dbsyncer.listener.mysql.MysqlExtractor;
import org.dbsyncer.listener.oracle.OracleExtractor;
import org.dbsyncer.listener.postgresql.PostgreSQLExtractor;
import org.dbsyncer.listener.sqlserver.DqlSqlServerExtractor;
import org.dbsyncer.listener.sqlserver.SqlServerExtractor;

/**
 * 日志模式支持类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum LogExtractorEnum {

    /**
     * Mysql
     */
    MYSQL(ConnectorEnum.MYSQL.getType(), MysqlExtractor.class),
    /**
     * Oracle
     */
    ORACLE(ConnectorEnum.ORACLE.getType(), OracleExtractor.class),
    /**
     * SqlServer
     */
    SQL_SERVER(ConnectorEnum.SQL_SERVER.getType(), SqlServerExtractor.class),
    /**
     * PostgreSQL
     */
    POSTGRE_SQL(ConnectorEnum.POSTGRE_SQL.getType(), PostgreSQLExtractor.class),
    /**
     * Kafka
     */
    KAFKA(ConnectorEnum.KAFKA.getType(), KafkaExtractor.class),
    /**
     * File
     */
    FILE(ConnectorEnum.FILE.getType(), FileExtractor.class),
    /**
     * DqlSqlServer
     */
    DQL_SQL_SERVER(ConnectorEnum.DQL_SQL_SERVER.getType(), DqlSqlServerExtractor.class);

    private String type;
    private Class clazz;

    LogExtractorEnum(String type, Class clazz) {
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
        for (LogExtractorEnum e : LogExtractorEnum.values()) {
            if (StringUtil.equals(type, e.getType())) {
                return e.getClazz();
            }
        }
        throw new ListenerException(String.format("LogExtractorEnum type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Class getClazz() {
        return clazz;
    }
}