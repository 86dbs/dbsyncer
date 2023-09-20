package org.dbsyncer.parser.ddl;

import org.dbsyncer.connector.config.DDLConfig;

public interface DDLParser {

    /**
     * 解析DDL配置
     *
     * @param sql 源表ALTER语句
     * @param targetConnectorType 目标连接器类型
     * @param targetTableName 目标表
     * @return
     */
    DDLConfig parseDDlConfig(String sql, String targetConnectorType, String targetTableName);
}