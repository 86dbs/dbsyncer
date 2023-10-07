package org.dbsyncer.parser.ddl;

import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.parser.model.FieldMapping;

import java.util.List;

public interface DDLParser {

    /**
     * 解析DDL配置
     *
     * @param sql                   源表ALTER语句
     * @param targetConnectorType   目标连接器类型
     * @param targetTableName       目标表
     * @param originalFieldMappings 字段映射关系
     * @return
     */
    DDLConfig parseDDlConfig(String sql, String targetConnectorType, String targetTableName, List<FieldMapping> originalFieldMappings);

    /**
     * 刷新字段映射关系（根据原来的映射关系和更改的字段进行新关系的映射组合）
     *
     * @param originalFieldMappings
     * @param originMetaInfo
     * @param targetMetaInfo
     * @param targetDDLConfig
     */
    List<FieldMapping> refreshFiledMappings(List<FieldMapping> originalFieldMappings, MetaInfo originMetaInfo, MetaInfo targetMetaInfo, DDLConfig targetDDLConfig);
}