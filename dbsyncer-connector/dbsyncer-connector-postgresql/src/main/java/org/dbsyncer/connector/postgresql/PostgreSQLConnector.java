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

import java.sql.Connection;
import java.sql.SQLException;
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

        // 从 MetaInfo 中提取字段列表和主键列表
        List<Field> fields = sourceMetaInfo.getColumn();
        List<String> primaryKeys = new ArrayList<>();
        for (Field field : fields) {
            if (field.isPk()) {
                primaryKeys.add(field.getName());
            }
        }

        // 调用 SqlTemplate 的 buildCreateTableSql 方法进行 SQL 模板组装
        // SqlTemplate 负责 SQL 语法和模板组装，Connector 只负责参数加工
        return sqlTemplate.buildCreateTableSql(null, targetTableName, fields, primaryKeys);
    }

    @Override
    protected CatalogAndSchema resolveEffectiveCatalogAndSchema(Connection conn, String catalog, String schema) throws SQLException {
        // PostgreSQL: schema=public 等，catalog=数据库名
        String effectiveCatalog = (catalog != null) ? catalog : conn.getCatalog();
        String effectiveSchema = (schema != null) ? schema : "public";
        return new CatalogAndSchema(effectiveCatalog, effectiveSchema);
    }

}