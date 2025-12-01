/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.connector.postgresql.cdc.PostgreSQLListener;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLBitValueMapper;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLOtherValueMapper;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.PostgreSQLTemplate;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public class PostgreSQLConnector extends AbstractDatabaseConnector {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PostgreSQLSchemaResolver schemaResolver = new PostgreSQLSchemaResolver();

    public PostgreSQLConnector() {
        VALUE_MAPPERS.put(Types.BIT, new PostgreSQLBitValueMapper());
        VALUE_MAPPERS.put(Types.OTHER, new PostgreSQLOtherValueMapper());
        sqlTemplate = new PostgreSQLTemplate(schemaResolver);
        configValidator = new PostgreSQLConfigValidator();
    }

    @Override
    public String getConnectorType() {
        return "PostgreSQL";
    }


    @Override
    public Map<String, String> getPosition(org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) throws Exception {
        // 查询当前WAL位置
        String currentLSN = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT pg_current_wal_lsn()", String.class));

        if (currentLSN != null) {
            // 创建与snapshot中存储格式一致的position信息
            Map<String, String> position = new HashMap<>();
            position.put("position", currentLSN);
            return position;
        }
        // 如果无法获取位置信息，返回空Map
        return new HashMap<>();
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new PostgreSQLListener();
        }
        return null;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        SqlTemplate sqlTemplate = this.sqlTemplate;
        if (sqlTemplate == null) {
            throw new UnsupportedOperationException("PostgreSQL连接器不支持自动生成 CREATE TABLE DDL");
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(sqlTemplate.buildTable(null, targetTableName)).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        for (Field sourceField : sourceMetaInfo.getColumn()) {
            // 1. 直接使用 SqlTemplate.convertToDatabaseType() 方法
            String ddlType = sqlTemplate.convertToDatabaseType(sourceField);

            // 2. 构建列定义
            String columnDef = "  " + sqlTemplate.buildColumn(sourceField.getName()) + " " + ddlType;
            columnDefs.add(columnDef);

            // 3. 收集主键
            if (sourceField.isPk()) {
                primaryKeys.add(sqlTemplate.buildColumn(sourceField.getName()));
            }
        }

        ddl.append(String.join(",\n", columnDefs));

        if (!primaryKeys.isEmpty()) {
            ddl.append(",\n  PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }

        ddl.append("\n)");
        return ddl.toString();
    }

}