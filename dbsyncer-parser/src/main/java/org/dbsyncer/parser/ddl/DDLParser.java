package org.dbsyncer.parser.ddl;

import java.util.List;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.model.FieldMapping;

public interface DDLParser {

    /**
     * 解析DDL配置
     *
     * @param sql
     * @param targetTableName
     * @param fieldMappingList 字段映射关系
     * @return
     */
    DDLConfig parseDDlConfig(String sql, String targetTableName, List<FieldMapping> fieldMappingList);
}