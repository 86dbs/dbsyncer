/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.cdc.OracleListener;
import org.dbsyncer.connector.oracle.schema.OracleClobValueMapper;
import org.dbsyncer.connector.oracle.schema.OracleNClobValueMapper;
import org.dbsyncer.connector.oracle.schema.OracleOtherValueMapper;
import org.dbsyncer.connector.oracle.validator.OracleConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
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

    private final OracleConfigValidator configValidator = new OracleConfigValidator();

    public OracleConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new OracleOtherValueMapper());
        VALUE_MAPPERS.put(Types.CLOB, new OracleClobValueMapper());
        VALUE_MAPPERS.put(Types.NCLOB, new OracleNClobValueMapper());
    }

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
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        // SELECT * FROM (SELECT A.*, ROWNUM RN FROM (select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid")A WHERE ROWNUM <= ?) WHERE RN > ?
        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
        if (PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            appendOrderByPk(config, sql);
        }
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            logger.debug("不支持游标查询，主键包含非数字类型");
            return StringUtil.EMPTY;
        }

        // SELECT * FROM (SELECT A.*, ROWNUM RN FROM (select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid")A WHERE ROWNUM <= ?) WHERE RN > ?
        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(config.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        final String quotation = buildSqlWithQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", skipFirst);
        appendOrderByPk(config, sql);
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
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors) {
            return new Object[]{pageSize, 0};
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 2];
        System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
        newCursors[cursorsLen] = pageSize;
        newCursors[cursorsLen + 1] = 0;
        return newCursors;
    }

    @Override
    public boolean enableCursor() {
        return true;
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
    }

}