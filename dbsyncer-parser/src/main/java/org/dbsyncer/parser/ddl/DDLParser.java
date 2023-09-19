package org.dbsyncer.parser.ddl;

import org.dbsyncer.connector.config.DDLConfig;

public interface DDLParser {

    /**
     * 解析DDL配置
     *
     * @param sql
     * @param targetTableName
     * @return
     */
    DDLConfig parseDDlConfig(String sql, String targetTableName);
}