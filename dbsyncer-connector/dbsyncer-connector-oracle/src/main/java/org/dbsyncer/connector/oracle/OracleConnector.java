/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.cdc.OracleListener;
import org.dbsyncer.connector.oracle.schema.OracleSchemaResolver;
import org.dbsyncer.connector.oracle.validator.OracleConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Oracle连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public final class OracleConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String QUERY_SCHEMA = "SELECT USERNAME FROM ALL_USERS where USERNAME not in('ANONYMOUS','APEX_030200','APEX_PUBLIC_USER','APPQOSSYS','BI','CTXSYS','DBSNMP','DIP','EXFSYS','FLOWS_FILES','HR','IX','MDDATA','MDSYS','MGMT_VIEW','OE','OLAPSYS','ORACLE_OCM','ORDDATA','ORDPLUGINS','ORDSYS','OUTLN','OWBSYS','OWBSYS_AUDIT','PM','SCOTT','SH','SI_INFORMTN_SCHEMA','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','SYS','SYSMAN','SYSTEM','WMSYS','XDB','XS$NULL') ORDER BY USERNAME";

    private final OracleConfigValidator configValidator = new OracleConfigValidator();
    private final OracleSchemaResolver schemaResolver = new OracleSchemaResolver();

    @Override
    public String getConnectorType() {
        return "Oracle";
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
            return new OracleListener();
        }
        return null;
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        // 使用三层嵌套查询：SELECT * FROM (SELECT A.*, ROWNUM RN FROM (...) A WHERE ROWNUM <= ?) WHERE RN > ?
        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
        // 使用基类方法添加ORDER BY（按主键排序，保证分页一致性）
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }

        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_CURSOR_SQL_START);
        sql.append(config.getQuerySql());
        // 使用基类的公共方法构建WHERE条件和ORDER BY
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.ORACLE_PAGE_CURSOR_SQL_END);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{pageSize};
        }
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{pageSize};
        }

        // Oracle不需要OFFSET，只需要添加pageSize参数
        Object[] newCursors = new Object[cursorArgs.length + 1];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = pageSize;
        return newCursors;
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
    }

    @Override
    protected String getCatalog(String database, Connection connection) {
        return null;
    }

    @Override
    protected String getSchema(String schema, Connection connection) throws SQLException {
        if (StringUtil.isBlank(schema)) {
            schema = connection.getSchema();
        }
        if (StringUtil.isNotBlank(schema)) {
            schema = schema.toUpperCase();
        }
        return schema;
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:oracle:thin:@127.0.0.1:1521:ORCL
        StringBuilder url = new StringBuilder();
        url.append("jdbc:oracle:thin:@").append(config.getHost()).append(":").append(config.getPort());
        String serviceName = config.getServiceName();
        if (StringUtil.isNotBlank(serviceName)) {
            // 使用Service Name
            url.append("/").append(serviceName);
        } else if (StringUtil.isNotBlank(database)) {
            // 使用SID (传统方式)
            url.append(":").append(database);
        } else {
            throw new IllegalArgumentException("Oracle需要指定serviceName或database(SID)");
        }

        return url.toString();
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }
}