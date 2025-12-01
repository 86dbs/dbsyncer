/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.connector.oracle.cdc.OracleListener;
import org.dbsyncer.connector.oracle.schema.OracleClobValueMapper;
import org.dbsyncer.connector.oracle.schema.OracleOtherValueMapper;
import org.dbsyncer.connector.oracle.schema.OracleSchemaResolver;
import org.dbsyncer.connector.oracle.validator.OracleConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.OracleTemplate;
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
 * Oracle连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public class OracleConnector extends AbstractDatabaseConnector {

    private final OracleSchemaResolver schemaResolver = new OracleSchemaResolver();

    private final Logger logger = LoggerFactory.getLogger(getClass());


    public OracleConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new OracleOtherValueMapper());
        VALUE_MAPPERS.put(Types.CLOB, new OracleClobValueMapper());
        sqlTemplate = new OracleTemplate(schemaResolver);
        configValidator = new OracleConfigValidator();
    }

    @Override
    public String getConnectorType() {
        return "Oracle";
    }

    @Override
    public Map<String, String> getPosition(org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) throws Exception {
        // 查询当前SCN号
        String currentSCN = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT CURRENT_SCN FROM V$DATABASE", String.class));

        if (currentSCN != null) {
            // 创建与snapshot中存储格式一致的position信息
            Map<String, String> position = new HashMap<>();
            position.put("position", currentSCN);
            logger.debug("成功获取Oracle当前SCN: {}", currentSCN);
            return position;
        }
        return null;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new OracleListener();
        }
        return null;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        SqlTemplate sqlTemplate = this.sqlTemplate;
        if (sqlTemplate == null) {
            throw new UnsupportedOperationException("Oracle连接器不支持自动生成 CREATE TABLE DDL");
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
}