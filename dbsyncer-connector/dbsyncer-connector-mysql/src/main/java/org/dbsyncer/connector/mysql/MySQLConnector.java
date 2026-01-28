/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql;

import org.dbsyncer.connector.mysql.cdc.MySQLListener;
import org.dbsyncer.connector.mysql.converter.IRToMySQLConverter;
import org.dbsyncer.connector.mysql.converter.MySQLToIRConverter;
import org.dbsyncer.connector.mysql.schema.MySQLDateValueMapper;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.connector.mysql.storage.MySQLStorageService;
import org.dbsyncer.connector.mysql.validator.MySQLConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.MySQLTemplate;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
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
 * MySQL连接器实现
 */
public class MySQLConnector extends AbstractDatabaseConnector {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final MySQLSchemaResolver schemaResolver = new MySQLSchemaResolver();


    public MySQLConnector() {
        sqlTemplate = new MySQLTemplate(schemaResolver);
        VALUE_MAPPERS.put(Types.DATE, new MySQLDateValueMapper());
        configValidator = new MySQLConfigValidator();
        sourceToIRConverter = new MySQLToIRConverter();
        irToTargetConverter = new IRToMySQLConverter(sqlTemplate);
    }

    @Override
    public String getConnectorType() {
        return "MySQL";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new MySQLListener();
        }
        return null;
    }

    @Override
    public StorageService getStorageService() {
        return new MySQLStorageService();
    }

    @Override
    public Map<String, String> getPosition(
            org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) throws Exception {
        // 执行SHOW MASTER STATUS命令获取当前binlog位置
        Map<String, Object> result = connectorInstance
                .execute(databaseTemplate -> databaseTemplate.queryForMap("SHOW MASTER STATUS"));

        if (result == null || result.isEmpty()) {
            throw new RuntimeException("获取MySQL当前binlog位置失败");
        }
        Map<String, String> position = new HashMap<>();
        position.put("fileName", (String) result.get("File"));
        position.put("position", String.valueOf(result.get("Position")));
        return position;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return Integer.MIN_VALUE; // MySQL流式处理特殊值
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        SqlTemplate sqlTemplate = this.sqlTemplate;
        if (sqlTemplate == null) {
            throw new UnsupportedOperationException("MySQL连接器不支持自动生成 CREATE TABLE DDL");
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
        // MySQL: schema=null, catalog=database name
        String effectiveCatalog = (catalog != null) ? catalog : conn.getCatalog();
        return new CatalogAndSchema(effectiveCatalog, null);
    }
}