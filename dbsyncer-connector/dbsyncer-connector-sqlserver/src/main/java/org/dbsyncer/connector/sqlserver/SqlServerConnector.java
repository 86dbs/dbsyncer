/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.sqlserver.cdc.Lsn;
import org.dbsyncer.connector.sqlserver.cdc.SqlServerListener;
import org.dbsyncer.connector.sqlserver.schema.SqlServerSchemaResolver;
import org.dbsyncer.connector.sqlserver.validator.SqlServerConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SqlServer连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class SqlServerConnector extends AbstractDatabaseConnector {
    private final String QUERY_DATABASE = "SELECT name FROM SYS.DATABASES WHERE database_id > 4 order by name";
    private final String QUERY_SCHEMA = "SELECT name FROM sys.schemas WHERE name NOT IN ('sys','INFORMATION_SCHEMA','db_owner','db_accessadmin','db_securityadmin','db_ddladmin','db_backupoperator','db_datareader','db_datawriter','db_denydatareader','db_denydatawriter') order by name";
    private final String QUERY_TABLE_IDENTITY = "select is_identity from sys.columns where object_id = object_id('%s') and is_identity > 0";
    private final String SET_TABLE_IDENTITY_ON = "set identity_insert %s.[%s] on;";
    private final String SET_TABLE_IDENTITY_OFF = ";set identity_insert %s.[%s] off;";

    private final SqlServerConfigValidator configValidator = new SqlServerConfigValidator();
    private final SqlServerSchemaResolver schemaResolver = new SqlServerSchemaResolver();

    @Override
    public String getConnectorType() {
        return "SqlServer";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new SqlServerListener();
        }
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_DATABASE, String.class));
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
    }

    @Override
    public String buildWithQuotation(String key) {
        return "[" + key + "]";
    }

    @Override
    public String getPageSql(PageSql config) {
        List<String> primaryKeys = buildPrimaryKeys(config.getPrimaryKeys());
        String orderBy = StringUtil.join(primaryKeys, StringUtil.COMMA);
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy, config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }

        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类的公共方法构建WHERE条件和ORDER BY
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.SQLSERVER_CURSOR_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{0, pageSize};
        }
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{0, pageSize};
        }
        
        // SQL Server使用 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY，参数顺序为 [游标参数..., 0, pageSize]
        Object[] newCursors = new Object[cursorArgs.length + 2];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = 0;  // OFFSET
        newCursors[cursorArgs.length + 1] = pageSize;  // FETCH NEXT
        return newCursors;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> targetCommand = super.getTargetCommand(commandConfig);
        String tableName = commandConfig.getTable().getName();
        // 判断表是否包含标识自增列
        DatabaseConnectorInstance db = (DatabaseConnectorInstance) commandConfig.getConnectorInstance();
        List<Integer> result = db.execute(databaseTemplate -> databaseTemplate.queryForList(String.format(QUERY_TABLE_IDENTITY, tableName), Integer.class));
        // 允许显式插入标识列的值
        if (!CollectionUtils.isEmpty(result)) {
            String insert = String.format(SET_TABLE_IDENTITY_ON, commandConfig.getSchema(), tableName)
                    + targetCommand.get(ConnectorConstant.OPERTION_INSERT)
                    + String.format(SET_TABLE_IDENTITY_OFF, commandConfig.getSchema(), tableName);
            targetCommand.put(ConnectorConstant.OPERTION_INSERT, insert);
        }
        return targetCommand;
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        Database database = config.getDatabase();
        
        List<String> fs = new ArrayList<>();
        List<String> sfs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        List<String> updateSets = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();

        config.getFields().forEach(f -> {
            String fieldName = database.buildWithQuotation(f.getName());
            fs.add(fieldName);
            sfs.add("s." + fieldName);
            // 处理特殊类型
            if (!database.buildCustomValue(vs, f)) {
                vs.add("?");
            }
            if (f.isPk()) {
                pkFieldNames.add(fieldName);
            } else {
                updateSets.add(String.format("%s = s.%s", fieldName, fieldName));
            }
        });
        
        StringBuilder sql = new StringBuilder(database.generateUniqueCode());
        sql.append("MERGE ").append(config.getSchema());
        sql.append(database.buildWithQuotation(config.getTableName())).append(" AS t ");
        sql.append("USING (VALUES (").append(StringUtil.join(vs, StringUtil.COMMA)).append(")) AS s (");
        sql.append(StringUtil.join(fs, StringUtil.COMMA)).append(") ");
        sql.append("ON ");
        
        // 构建 ON 条件：t.pk = s.pk AND ...
        StringBuilder onCondition = new StringBuilder();
        for (int i = 0; i < pkFieldNames.size(); i++) {
            if (i > 0) {
                onCondition.append(" AND ");
            }
            String pkFieldName = pkFieldNames.get(i);
            onCondition.append("t.").append(pkFieldName).append(" = s.").append(pkFieldName);
        }
        sql.append(onCondition).append(" ");
        
        sql.append("WHEN MATCHED THEN UPDATE SET ");
        sql.append(StringUtil.join(updateSets, StringUtil.COMMA)).append(" ");
        sql.append("WHEN NOT MATCHED THEN INSERT (");
        sql.append(StringUtil.join(fs, StringUtil.COMMA)).append(") VALUES (");
        sql.append(StringUtil.join(sfs, StringUtil.COMMA)).append(");");
        
        return sql.toString();
    }

    @Override
    public Object getPosition(DatabaseConnectorInstance connectorInstance) {
        String sql = "SELECT * from cdc.lsn_time_mapping order by tran_begin_time desc";
        List<Map<String, Object>> result = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(sql));
        if (!CollectionUtils.isEmpty(result)) {
            List<Object> list = new ArrayList<>();
            result.forEach(r -> {
                r.computeIfPresent("start_lsn", (k, lsn)-> new Lsn((byte[]) lsn).toString());
                r.computeIfPresent("tran_begin_lsn", (k, lsn)-> new Lsn((byte[]) lsn).toString());
                r.computeIfPresent("tran_id", (k, lsn)-> new Lsn((byte[]) lsn).toString());
                r.computeIfPresent("tran_begin_time", (k, tranBeginTime)-> DateFormatUtil.timestampToString((Timestamp) tranBeginTime));
                r.computeIfPresent("tran_end_time", (k, tranEndTime)-> DateFormatUtil.timestampToString((Timestamp) tranEndTime));
                list.add(r);
            });
            return list;
        }
        return result;
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return StringUtil.isNotBlank(schema) ? schema : "dbo";
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:sqlserver://127.0.0.1:1433;databaseName=test;encrypt=false;trustServerCertificate=true
        StringBuilder url = new StringBuilder();
        url.append("jdbc:sqlserver://").append(config.getHost()).append(":").append(config.getPort());
        if (StringUtil.isNotBlank(database)) {
            url.append(";databaseName=").append(database);
        }
        return url.toString();
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public boolean buildCustom(List<String> fs, Field field) {
        switch (field.getTypeName()) {
            /**
             * SRID	    名称	                    用途	                        单位
             * 0	    未定义/本地坐标系	        SQL Server geometry 默认	    任意单位
             * 4326	    WGS84	                GPS、全球坐标系	            度（经纬度）
             * 3857	    Web Mercator	        Google Maps、OpenStreetMap	米
             * 4490	    CGCS2000	            中国2000坐标系	            度
             * 4547	    CGCS2000/Gauss-Kruger	中国投影坐标系	            米
             * 26910	NAD83/UTM zone 10N	    北美坐标系	                米
             * 32610	WGS84/UTM zone 10N	    全球UTM投影	                米
             */
            case "geometry":
            case "geography":
                // 使用 STAsText() 获取 WKT 格式，同时包含 SRID 信息，格式：POINT (...) | 4326
                fs.add(String.format("CAST([%s].STAsText() AS NVARCHAR(MAX)) + ' | ' + CAST([%s].STSrid AS NVARCHAR(10)) AS [%s]",
                    field.getName(), field.getName(), field.getName()));
                return true;
            default:
                break;
        }
        return super.buildCustom(fs, field);
    }

    @Override
    public boolean buildCustomValue(List<String> fs, Field field) {
        switch (field.getTypeName()) {
            case "geometry":
                // POINT (133.4 38.5) | 4326
                // 处理 NULL 值：如果第一个参数（WKT）为 NULL，则返回 NULL，否则调用 STGeomFromText
                // 注意：第一个 ? 用于 NULL 检查，第二个和第三个 ? 是 WKT 和 SRID 参数
                fs.add("CASE WHEN ? IS NULL THEN CAST(NULL AS geometry) ELSE geometry::STGeomFromText(?, ?) END");
                return true;
            case "geography":
                // POINT (133.4 38.5) | 4326
                // 处理 NULL 值：如果第一个参数（WKT）为 NULL，则返回 NULL，否则调用 STGeomFromText
                fs.add("CASE WHEN ? IS NULL THEN CAST(NULL AS geography) ELSE geography::STGeomFromText(?, ?) END");
                return true;
            default:
                break;
        }
        return super.buildCustomValue(fs, field);
    }
}